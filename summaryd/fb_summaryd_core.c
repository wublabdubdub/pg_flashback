#include "postgres.h"

#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "storage/fd.h"

#include "fb_runtime.h"
#include "fb_summary.h"
#include "summaryd/fb_summaryd_core.h"

#define SUMMARYD_CORE_HOT_WINDOW_MIN 32
#define SUMMARYD_CORE_HOT_WINDOW_PER_WORKER 16
#define SUMMARYD_CORE_RECENT_PROTECT_SECS 5
#define SUMMARYD_CORE_META_LIMIT_MB 256
#define SUMMARYD_CORE_META_LOW_WATERMARK_PERCENT 80
#define SUMMARYD_CORE_RATE_WINDOW_MS (5 * 60 * 1000)

typedef struct SummarydCleanupEntry
{
	char path[MAXPGPATH];
	uint64 file_identity_hash;
	uint64 size_bytes;
	time_t mtime_sec;
	long mtime_nsec;
} SummarydCleanupEntry;

static int
summaryd_core_hot_window(int registered_workers)
{
	int workers = Max(registered_workers, 1);

	return Max(SUMMARYD_CORE_HOT_WINDOW_MIN,
			   workers * SUMMARYD_CORE_HOT_WINDOW_PER_WORKER);
}

static bool
summaryd_core_hash_in_list(const uint64 *hashes, int hash_count, uint64 target_hash)
{
	int i;

	if (hashes == NULL || hash_count <= 0 || target_hash == 0)
		return false;

	for (i = 0; i < hash_count; i++)
	{
		if (hashes[i] == target_hash)
			return true;
	}
	return false;
}

static void
summaryd_core_collect_candidate_hashes(const FbSummaryBuildCandidate *candidates,
										 int candidate_count,
										 uint64 **hashes_out,
										 int *count_out)
{
	uint64 *hashes = NULL;
	int hash_count = 0;
	int hash_capacity = 0;
	int i;

	if (hashes_out != NULL)
		*hashes_out = NULL;
	if (count_out != NULL)
		*count_out = 0;

	for (i = 0; i < candidate_count; i++)
	{
		uint64 hash = fb_summary_candidate_identity_hash(&candidates[i]);

		if (hash == 0 || summaryd_core_hash_in_list(hashes, hash_count, hash))
			continue;
		if (hash_count >= hash_capacity)
		{
			hash_capacity = (hash_capacity == 0) ? 128 : (hash_capacity * 2);
			hashes = (hashes == NULL) ?
				palloc(sizeof(uint64) * hash_capacity) :
				repalloc(hashes, sizeof(uint64) * hash_capacity);
		}
		hashes[hash_count++] = hash;
	}

	if (hashes_out != NULL)
		*hashes_out = hashes;
	else if (hashes != NULL)
		pfree(hashes);
	if (count_out != NULL)
		*count_out = hash_count;
}

static bool
summaryd_core_parse_summary_filename(const char *name, uint64 *hash_out)
{
	unsigned long long parsed_hash = 0;
	int consumed = 0;

	if (hash_out != NULL)
		*hash_out = 0;
	if (name == NULL)
		return false;
	if (sscanf(name, "summary-%16llx.meta%n", &parsed_hash, &consumed) != 1)
		return false;
	if (consumed <= 0 || name[consumed] != '\0')
		return false;
	if (hash_out != NULL)
		*hash_out = (uint64) parsed_hash;
	return true;
}

static int
summaryd_core_cleanup_entry_cmp(const void *lhs, const void *rhs)
{
	const SummarydCleanupEntry *left = (const SummarydCleanupEntry *) lhs;
	const SummarydCleanupEntry *right = (const SummarydCleanupEntry *) rhs;

	if (left->mtime_sec < right->mtime_sec)
		return -1;
	if (left->mtime_sec > right->mtime_sec)
		return 1;
	if (left->mtime_nsec < right->mtime_nsec)
		return -1;
	if (left->mtime_nsec > right->mtime_nsec)
		return 1;
	if (left->file_identity_hash < right->file_identity_hash)
		return -1;
	if (left->file_identity_hash > right->file_identity_hash)
		return 1;
	return 0;
}

static uint64
summaryd_core_meta_limit_bytes(void)
{
	return (uint64) SUMMARYD_CORE_META_LIMIT_MB * UINT64CONST(1024) * UINT64CONST(1024);
}

static uint64
summaryd_core_meta_low_bytes(void)
{
	return (summaryd_core_meta_limit_bytes() *
			(uint64) SUMMARYD_CORE_META_LOW_WATERMARK_PERCENT) / 100;
}

static bool
summaryd_core_candidate_in_snapshot(TimeLineID snapshot_timeline_id,
									XLogSegNo snapshot_oldest_segno,
									XLogSegNo snapshot_newest_segno,
									const FbSummaryBuildCandidate *candidate)
{
	if (candidate == NULL || snapshot_timeline_id == 0 ||
		snapshot_oldest_segno == 0 || snapshot_newest_segno == 0)
		return false;

	return candidate->timeline_id == snapshot_timeline_id &&
		candidate->segno >= snapshot_oldest_segno &&
		candidate->segno <= snapshot_newest_segno;
}

static void
summaryd_core_collect_snapshot_hashes(TimeLineID snapshot_timeline_id,
									  XLogSegNo snapshot_oldest_segno,
									  XLogSegNo snapshot_newest_segno,
									  uint64 **hashes_out,
									  int *count_out)
{
	FbSummaryBuildCandidate *candidates = NULL;
	uint64 *hashes = NULL;
	int hash_capacity = 0;
	int hash_count = 0;
	int candidate_count;
	int i;

	if (hashes_out != NULL)
		*hashes_out = NULL;
	if (count_out != NULL)
		*count_out = 0;

	candidate_count = fb_summary_collect_build_candidates(&candidates, true);
	for (i = 0; i < candidate_count; i++)
	{
		uint64 hash;

		if (!summaryd_core_candidate_in_snapshot(snapshot_timeline_id,
												  snapshot_oldest_segno,
												  snapshot_newest_segno,
												  &candidates[i]))
			continue;
		hash = fb_summary_candidate_identity_hash(&candidates[i]);
		if (hash == 0 || summaryd_core_hash_in_list(hashes, hash_count, hash))
			continue;
		if (hash_count >= hash_capacity)
		{
			hash_capacity = (hash_capacity == 0) ? 128 : (hash_capacity * 2);
			hashes = (hashes == NULL) ?
				palloc(sizeof(uint64) * hash_capacity) :
				repalloc(hashes, sizeof(uint64) * hash_capacity);
		}
		hashes[hash_count++] = hash;
	}
	fb_summary_free_build_candidates(candidates);

	if (hashes_out != NULL)
		*hashes_out = hashes;
	else if (hashes != NULL)
		pfree(hashes);
	if (count_out != NULL)
		*count_out = hash_count;
}

static unsigned long long
summaryd_core_run_cleanup(TimeLineID snapshot_timeline_id,
						  XLogSegNo snapshot_oldest_segno,
						  XLogSegNo snapshot_newest_segno)
{
	char *summary_dir;
	DIR *dir;
	struct dirent *de;
	FbSummaryBuildCandidate *candidates = NULL;
	SummarydCleanupEntry *entries = NULL;
	uint64 *live_hashes = NULL;
	uint64 *protected_hashes = NULL;
	int live_hash_count = 0;
	int protected_count = 0;
	int entry_count = 0;
	int entry_capacity = 0;
	int candidate_count;
	int i;
	time_t now_sec = time(NULL);
	uint64 total_bytes;
	uint64 low_bytes;
	unsigned long long cleanup_count = 0;

	total_bytes = fb_summary_meta_summary_size_bytes(NULL);
	low_bytes = summaryd_core_meta_low_bytes();
	candidate_count = fb_summary_collect_build_candidates(&candidates, false);
	summaryd_core_collect_candidate_hashes(candidates,
											 candidate_count,
											 &live_hashes,
											 &live_hash_count);
	summaryd_core_collect_snapshot_hashes(snapshot_timeline_id,
											 snapshot_oldest_segno,
											 snapshot_newest_segno,
											 &protected_hashes,
											 &protected_count);

	summary_dir = fb_runtime_meta_summary_dir();
	dir = AllocateDir(summary_dir);
	if (dir == NULL)
	{
		fb_summary_free_build_candidates(candidates);
		if (live_hashes != NULL)
			pfree(live_hashes);
		if (protected_hashes != NULL)
			pfree(protected_hashes);
		pfree(summary_dir);
		return 0;
	}

	while ((de = ReadDir(dir, summary_dir)) != NULL)
	{
		char path[MAXPGPATH];
		struct stat st;
		uint64 parsed_hash = 0;

		if (!summaryd_core_parse_summary_filename(de->d_name, &parsed_hash))
			continue;

		snprintf(path, sizeof(path), "%s/%s", summary_dir, de->d_name);
		if (stat(path, &st) != 0)
			continue;
		if ((now_sec - st.st_mtime) < SUMMARYD_CORE_RECENT_PROTECT_SECS)
			continue;

		if (!summaryd_core_hash_in_list(live_hashes, live_hash_count, parsed_hash))
		{
			if (unlink(path) == 0)
			{
				total_bytes -= Min(total_bytes, (uint64) st.st_size);
				cleanup_count++;
			}
			continue;
		}

		if (summaryd_core_hash_in_list(protected_hashes, protected_count, parsed_hash))
			continue;

		if (entry_count == entry_capacity)
		{
			entry_capacity = (entry_capacity == 0) ? 64 : (entry_capacity * 2);
			entries = (entries == NULL) ?
				palloc(sizeof(SummarydCleanupEntry) * entry_capacity) :
				repalloc(entries, sizeof(SummarydCleanupEntry) * entry_capacity);
		}

		strlcpy(entries[entry_count].path, path, sizeof(entries[entry_count].path));
		entries[entry_count].file_identity_hash = parsed_hash;
		entries[entry_count].size_bytes = (uint64) st.st_size;
		entries[entry_count].mtime_sec = st.st_mtime;
#if defined(__APPLE__)
		entries[entry_count].mtime_nsec = st.st_mtimespec.tv_nsec;
#else
		entries[entry_count].mtime_nsec = 0;
#endif
		entry_count++;
	}

	FreeDir(dir);
	pfree(summary_dir);
	fb_summary_free_build_candidates(candidates);
	if (live_hashes != NULL)
		pfree(live_hashes);
	if (protected_hashes != NULL)
		pfree(protected_hashes);

	if (total_bytes <= summaryd_core_meta_limit_bytes() || entry_count == 0)
	{
		if (entries != NULL)
			pfree(entries);
		return cleanup_count;
	}

	qsort(entries, entry_count, sizeof(SummarydCleanupEntry),
		  summaryd_core_cleanup_entry_cmp);
	for (i = 0; i < entry_count && total_bytes > low_bytes; i++)
	{
		if (unlink(entries[i].path) != 0)
			continue;
		total_bytes -= Min(total_bytes, entries[i].size_bytes);
		cleanup_count++;
	}

	pfree(entries);
	return cleanup_count;
}

static bool
summaryd_core_try_build_candidate(const FbSummaryBuildCandidate *candidate)
{
	bool built = false;

	summaryd_core_clear_error();
	PG_TRY();
	{
		built = fb_summary_build_candidate(candidate);
	}
	PG_CATCH();
	{
		built = false;
	}
	PG_END_TRY();

	return built;
}

static bool
summaryd_core_timestamp_difference_exceeds(TimestampTz start,
										   TimestampTz stop,
										   int msec)
{
	int64 threshold;

	if (start == 0 || stop == 0 || stop <= start)
		return false;
	threshold = (int64) msec * INT64CONST(1000);
	return (stop - start) > threshold;
}

bool
summaryd_run_core_iteration(const SummarydConfig *config,
								 SummarydState *state,
								 SummarydStatePublishHook publish_hook)
{
	FbSummaryBuildCandidate *candidates = NULL;
	int candidate_count;
	int hot_window;
	int hot_candidates;
	unsigned long long cleanup_count = 0;
	int i;
	bool ok = true;
	TimestampTz throughput_window_started_at =
		(TimestampTz) state->throughput_window_started_at_internal;
	TimestampTz last_build_at =
		(TimestampTz) state->last_build_at_internal;
	uint64 throughput_window_builds = (uint64) state->throughput_window_builds;

	if (config == NULL || state == NULL)
		return false;

	summaryd_core_set_paths(config);
	candidate_count = fb_summary_collect_build_candidates(&candidates, false);
	hot_window = summaryd_core_hot_window(1);
	hot_candidates = Min(hot_window, candidate_count);

	state->service_enabled = true;
	state->daemon_pid = getpid();
	state->registered_workers = 1;
	state->active_workers = 1;
	state->queue_capacity = 0;
	state->hot_window = hot_window;
	state->pending_hot = 0;
	state->pending_cold = 0;
	state->running_hot = 0;
	state->running_cold = 0;
	state->snapshot_timeline_id =
		(candidate_count > 0) ? candidates[candidate_count - 1].timeline_id : 0;
	state->snapshot_oldest_segno =
		(candidate_count > 0) ? (unsigned long long) candidates[0].segno : 0;
	state->snapshot_newest_segno =
		(candidate_count > 0) ?
		(unsigned long long) candidates[candidate_count - 1].segno : 0;
	state->snapshot_hot_candidates = hot_candidates;
	state->snapshot_cold_candidates = Max(candidate_count - hot_candidates, 0);
	state->scan_count++;
	state->enqueue_count = 0;
	state->last_error[0] = '\0';
	state->throughput_window_builds = throughput_window_builds;
	state->last_scan_at_internal = (long long) GetCurrentTimestamp();
	if (publish_hook != NULL)
		(void) publish_hook(config, state);

	for (i = 0; i < candidate_count; i++)
	{
		if (fb_summary_candidate_summary_exists(&candidates[i]))
			continue;
		if (!summaryd_core_try_build_candidate(&candidates[i]))
		{
			ok = false;
			state->service_enabled = false;
			strlcpy(state->last_error,
					summaryd_core_last_error(),
					sizeof(state->last_error));
			break;
		}
		{
			TimestampTz now = GetCurrentTimestamp();

			if (throughput_window_started_at == 0 ||
				summaryd_core_timestamp_difference_exceeds(throughput_window_started_at,
														   now,
														   SUMMARYD_CORE_RATE_WINDOW_MS))
			{
				throughput_window_started_at = now;
				throughput_window_builds = 1;
			}
			else
				throughput_window_builds++;
			last_build_at = now;
			state->throughput_window_started_at_internal =
				(long long) throughput_window_started_at;
			state->last_build_at_internal = (long long) last_build_at;
			state->throughput_window_builds = throughput_window_builds;
		}
		state->build_count++;
		if (publish_hook != NULL)
			(void) publish_hook(config, state);
	}

	if (throughput_window_started_at == 0)
		state->throughput_window_started_at_internal = 0;
	if (last_build_at == 0)
		state->last_build_at_internal = 0;

	cleanup_count = summaryd_core_run_cleanup((TimeLineID) state->snapshot_timeline_id,
												 (XLogSegNo) state->snapshot_oldest_segno,
												 (XLogSegNo) state->snapshot_newest_segno);

	state->cleanup_count += cleanup_count;
	if (ok)
		state->last_error[0] = '\0';
	if (publish_hook != NULL)
		(void) publish_hook(config, state);

	fb_summary_free_build_candidates(candidates);
	return ok;
}
