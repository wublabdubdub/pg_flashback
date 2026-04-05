/*
 * fb_summary_service.c
 *    Background summary prebuild launcher and workers.
 */

#include "postgres.h"

#include "common/int.h"
#include <dirent.h>
#include <math.h>
#include <signal.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "catalog/pg_type_d.h"
#include "funcapi.h"
#include "fmgr.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "fb_guc.h"
#include "fb_runtime.h"
#include "fb_summary.h"
#include "fb_summary_service.h"

#define FB_SUMMARY_SERVICE_TRANCHE "fb_summary_service"
#define FB_SUMMARY_SERVICE_SHMEM_NAME "fb_summary_service_shmem"
#define FB_SUMMARY_SERVICE_RECENT_PROTECT_SECS 5
#define FB_SUMMARY_SERVICE_HOT_WINDOW_MIN 32
#define FB_SUMMARY_SERVICE_HOT_WINDOW_PER_WORKER 16
#define FB_SUMMARY_SERVICE_COLD_BATCH_SIZE 8
#define FB_SUMMARY_SERVICE_RATE_WINDOW_MS (5 * 60 * 1000)
#define FB_SUMMARY_SERVICE_RATE_MIN_SAMPLE_MS 5000

PG_FUNCTION_INFO_V1(fb_summary_progress_internal);
PG_FUNCTION_INFO_V1(fb_summary_service_debug_internal);
PG_FUNCTION_INFO_V1(fb_summary_cleanup_plan_debug);
PG_FUNCTION_INFO_V1(fb_summary_service_plan_debug);
PG_FUNCTION_INFO_V1(fb_summary_service_schedule_debug);
PG_FUNCTION_INFO_V1(fb_summary_service_worker_error_isolation_debug);
PG_FUNCTION_INFO_V1(fb_summary_service_cleanup_debug);
PG_FUNCTION_INFO_V1(fb_summary_service_memory_reset_debug);

PGDLLEXPORT Datum fb_summary_progress_internal(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum fb_summary_service_debug_internal(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum fb_summary_cleanup_plan_debug(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum fb_summary_service_plan_debug(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum fb_summary_service_schedule_debug(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum fb_summary_service_worker_error_isolation_debug(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum fb_summary_service_cleanup_debug(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum fb_summary_service_memory_reset_debug(PG_FUNCTION_ARGS);

typedef enum FbSummaryServiceTaskState
{
	FB_SUMMARY_SERVICE_TASK_EMPTY = 0,
	FB_SUMMARY_SERVICE_TASK_PENDING,
	FB_SUMMARY_SERVICE_TASK_RUNNING
} FbSummaryServiceTaskState;

typedef enum FbSummaryServiceQueueKind
{
	FB_SUMMARY_SERVICE_QUEUE_HOT = 1,
	FB_SUMMARY_SERVICE_QUEUE_COLD
} FbSummaryServiceQueueKind;

typedef struct FbSummaryServiceTask
{
	int state;
	int queue_kind;
	int recent_rank;
	uint64 file_identity_hash;
	pid_t owner_pid;
	int attempt_count;
	TimestampTz lease_started_at;
	FbSummaryBuildCandidate candidate;
} FbSummaryServiceTask;

typedef struct FbSummaryServiceShared
{
	int queue_capacity;
	pid_t launcher_pid;
	pid_t worker_pids[8];
	int snapshot_valid;
	TimeLineID snapshot_timeline_id;
	XLogSegNo snapshot_oldest_segno;
	XLogSegNo snapshot_newest_segno;
	int snapshot_stable_candidates;
	int snapshot_hot_candidates;
	uint64 scan_count;
	uint64 enqueue_count;
	uint64 build_count;
	uint64 cleanup_count;
	TimestampTz last_scan_at;
	TimestampTz throughput_window_started_at;
	TimestampTz last_build_at;
	uint64 throughput_window_builds;
	TimestampTz last_query_observed_at;
	uint32 last_query_summary_span_fallback_segments;
	uint32 last_query_metadata_fallback_segments;
	FbSummaryServiceTask tasks[FLEXIBLE_ARRAY_MEMBER];
} FbSummaryServiceShared;

typedef struct FbSummaryServicePlanDebugContext
{
	FbSummaryBuildCandidate *candidates;
	bool *enqueued;
	int *enqueue_order;
	int candidate_count;
	int hot_window;
	int index;
} FbSummaryServicePlanDebugContext;

typedef struct FbSummaryCleanupPlanDebugContext
{
	FbSummaryBuildCandidate *candidates;
	int candidate_count;
	uint64 *active_hashes;
	int active_count;
	bool snapshot_valid;
	TimeLineID snapshot_timeline_id;
	XLogSegNo snapshot_oldest_segno;
	XLogSegNo snapshot_newest_segno;
	int index;
} FbSummaryCleanupPlanDebugContext;

typedef struct FbSummaryServiceScheduleDebugContext
{
	int total_candidates;
	int hot_window;
	int queue_capacity;
	bool *enqueued;
	int *enqueue_order;
	int index;
} FbSummaryServiceScheduleDebugContext;

typedef struct FbSummaryServiceProgressStats
{
	bool service_enabled;
	int queue_capacity;
	int registered_workers;
	int active_workers;
	int hot_window;
	int pending_hot;
	int pending_cold;
	int running_hot;
	int running_cold;
	TimeLineID snapshot_timeline_id;
	XLogSegNo snapshot_oldest_segno;
	XLogSegNo snapshot_newest_segno;
	XLogSegNo visible_oldest_segno;
	XLogSegNo visible_newest_segno;
	int snapshot_hot_candidates;
	int snapshot_cold_candidates;
	pid_t launcher_pid;
	uint64 scan_count;
	uint64 enqueue_count;
	uint64 build_count;
	uint64 cleanup_count;
	TimestampTz last_scan_at;
	TimestampTz throughput_window_started_at;
	TimestampTz last_build_at;
	uint64 throughput_window_builds;
	uint64 stable_candidates;
	uint64 completed_summaries;
	uint64 missing_summaries;
	uint64 hot_missing_summaries;
	uint64 cold_missing_summaries;
	TimestampTz stable_oldest_ts;
	TimestampTz stable_newest_ts;
	TimestampTz near_contiguous_through_ts;
	TimestampTz far_contiguous_until_ts;
	XLogSegNo first_gap_from_newest_segno;
	TimestampTz first_gap_from_newest_ts;
	XLogSegNo first_gap_from_oldest_segno;
	TimestampTz first_gap_from_oldest_ts;
	uint32 summary_files;
	uint64 summary_bytes;
	TimestampTz last_query_observed_at;
	TimestampTz estimated_completion_at;
	bool last_query_summary_ready;
	uint32 last_query_summary_span_fallback_segments;
	uint32 last_query_metadata_fallback_segments;
} FbSummaryServiceProgressStats;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static FbSummaryServiceShared *fb_summary_service = NULL;
static LWLock *fb_summary_service_lock = NULL;
static volatile sig_atomic_t fb_summary_service_got_sigterm = false;

PGDLLEXPORT void fb_summary_service_launcher_main(Datum main_arg);
PGDLLEXPORT void fb_summary_service_worker_main(Datum main_arg);

static Size fb_summary_service_shmem_size(void);
static void fb_summary_service_shmem_request(void);
static void fb_summary_service_shmem_startup(void);
static void fb_summary_service_register_bgworkers(void);
static int fb_summary_service_registered_workers(void);
static void fb_summary_service_sigterm(SIGNAL_ARGS);
static int fb_summary_service_hot_window(void);
static int fb_summary_service_recent_rank_for_index(int candidate_index, int candidate_count);
static int fb_summary_service_queue_kind_for_rank(int recent_rank, int hot_window);
static const char *fb_summary_service_queue_kind_name(int queue_kind);
static int fb_summary_service_compare_priority(int queue_kind_a,
												 TimeLineID timeline_id_a,
												 XLogSegNo segno_a,
												 int recent_rank_a,
												 int queue_kind_b,
												 TimeLineID timeline_id_b,
												 XLogSegNo segno_b,
												 int recent_rank_b);
static bool fb_summary_service_worker_pid_known_locked(pid_t owner_pid);
static void fb_summary_service_recover_stale_tasks_locked(void);
static FbSummaryServiceTask *fb_summary_service_find_task_locked(uint64 file_identity_hash);
static bool fb_summary_service_candidate_summary_exists_fast(const FbSummaryBuildCandidate *candidate);
static bool fb_summary_service_candidate_summary_exists_for_enqueue(const FbSummaryBuildCandidate *candidate);
static bool fb_summary_service_try_enqueue(const FbSummaryBuildCandidate *candidate,
											 int queue_kind,
											 int recent_rank);
static int fb_summary_service_claim_tasks(FbSummaryBuildCandidate *candidates_out,
										   int *slot_indexes_out,
										   int max_count,
										   int preferred_queue_kind,
										   bool allow_fallback_queue);
static void fb_summary_service_finish_task(int slot_index, bool built);
static void fb_summary_service_run_scan(void);
static void fb_summary_service_run_cleanup(void);
static void fb_summary_service_collect_progress(FbSummaryServiceProgressStats *stats);
static void fb_summary_service_compute_eta(FbSummaryServiceProgressStats *stats);
static bool fb_summary_service_build_candidate_safe(const FbSummaryBuildCandidate *candidate);
static bool fb_summary_service_candidate_in_snapshot(bool snapshot_valid,
													 TimeLineID snapshot_timeline_id,
													 XLogSegNo snapshot_oldest_segno,
													 XLogSegNo snapshot_newest_segno,
													 const FbSummaryBuildCandidate *candidate);
static void fb_summary_service_collect_candidate_hashes(const FbSummaryBuildCandidate *candidates,
														 int candidate_count,
														 uint64 **hashes_out,
														 int *count_out);
static bool fb_summary_service_hash_in_list(const uint64 *hashes,
											 int hash_count,
											 uint64 target_hash);
static bool fb_summary_service_parse_summary_filename(const char *name,
													   uint64 *hash_out);
static void fb_summary_service_prune_missing_pending_tasks_locked(const uint64 *live_hashes,
																   int live_count);
static void fb_summary_service_collect_protected_hashes(uint64 **hashes_out,
														 int *count_out,
														 bool include_snapshot);

void
fb_summary_service_report_query_summary_usage(TimestampTz observed_at,
												   uint32 summary_span_fallback_segments,
												   uint32 metadata_fallback_segments)
{
	if (!fb_summary_service_enabled() ||
		fb_summary_service == NULL ||
		fb_summary_service_lock == NULL)
		return;

	LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
	fb_summary_service->last_query_observed_at = observed_at;
	fb_summary_service->last_query_summary_span_fallback_segments =
		summary_span_fallback_segments;
	fb_summary_service->last_query_metadata_fallback_segments =
		metadata_fallback_segments;
	LWLockRelease(fb_summary_service_lock);
}

void
fb_summary_service_shmem_init(void)
{
	if (!fb_summary_service_enabled())
		return;

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = fb_summary_service_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = fb_summary_service_shmem_startup;

	fb_summary_service_register_bgworkers();
}

static Size
fb_summary_service_shmem_size(void)
{
	return MAXALIGN(sizeof(FbSummaryServiceShared)) +
		MAXALIGN(sizeof(FbSummaryServiceTask) * fb_summary_service_queue_size());
}

static void
fb_summary_service_shmem_request(void)
{
	if (prev_shmem_request_hook != NULL)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(fb_summary_service_shmem_size());
	RequestNamedLWLockTranche(FB_SUMMARY_SERVICE_TRANCHE, 1);
}

static void
fb_summary_service_shmem_startup(void)
{
	bool found = false;

	if (prev_shmem_startup_hook != NULL)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	fb_summary_service = ShmemInitStruct(FB_SUMMARY_SERVICE_SHMEM_NAME,
										 fb_summary_service_shmem_size(),
										 &found);
	fb_summary_service_lock = &(GetNamedLWLockTranche(FB_SUMMARY_SERVICE_TRANCHE))[0].lock;
	if (!found)
	{
		MemSet(fb_summary_service, 0, fb_summary_service_shmem_size());
		fb_summary_service->queue_capacity = fb_summary_service_queue_size();
	}
	LWLockRelease(AddinShmemInitLock);
}

static void
fb_summary_service_register_bgworkers(void)
{
	BackgroundWorker worker;
	int worker_count;
	int i;

	worker_count = fb_summary_service_registered_workers();

	MemSet(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 5;
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_flashback summary launcher");
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_flashback summary launcher");
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_flashback");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "fb_summary_service_launcher_main");
	worker.bgw_main_arg = (Datum) 0;
	RegisterBackgroundWorker(&worker);

	for (i = 0; i < worker_count; i++)
	{
		MemSet(&worker, 0, sizeof(worker));
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = 5;
		snprintf(worker.bgw_name, BGW_MAXLEN, "pg_flashback summary worker %d", i + 1);
		snprintf(worker.bgw_type, BGW_MAXLEN, "pg_flashback summary worker");
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_flashback");
		snprintf(worker.bgw_function_name, BGW_MAXLEN, "fb_summary_service_worker_main");
		worker.bgw_main_arg = Int32GetDatum(i);
		RegisterBackgroundWorker(&worker);
	}
}

static int
fb_summary_service_registered_workers(void)
{
	char *value;
	int max_worker_processes = 8;
	int reserved_for_queries = 5;
	int allowed;

	value = GetConfigOptionByName("max_worker_processes", NULL, false);
	if (value != NULL && value[0] != '\0')
		max_worker_processes = atoi(value);

	allowed = max_worker_processes - reserved_for_queries;
	if (allowed < 1)
		allowed = 1;
	return Min(fb_summary_service_workers(), allowed);
}

static int
fb_summary_service_hot_window(void)
{
	int workers = fb_summary_service_registered_workers();

	if (workers < 1)
		workers = 1;
	return Max(FB_SUMMARY_SERVICE_HOT_WINDOW_MIN,
			   workers * FB_SUMMARY_SERVICE_HOT_WINDOW_PER_WORKER);
}

static int
fb_summary_service_recent_rank_for_index(int candidate_index, int candidate_count)
{
	return candidate_count - candidate_index;
}

static int
fb_summary_service_queue_kind_for_rank(int recent_rank, int hot_window)
{
	if (recent_rank <= hot_window)
		return FB_SUMMARY_SERVICE_QUEUE_HOT;
	return FB_SUMMARY_SERVICE_QUEUE_COLD;
}

static const char *
fb_summary_service_queue_kind_name(int queue_kind)
{
	switch (queue_kind)
	{
		case FB_SUMMARY_SERVICE_QUEUE_HOT:
			return "hot";
		case FB_SUMMARY_SERVICE_QUEUE_COLD:
			return "cold";
		default:
			return "unknown";
	}
}

static int
fb_summary_service_compare_priority(int queue_kind_a,
									  TimeLineID timeline_id_a,
									  XLogSegNo segno_a,
									  int recent_rank_a,
									  int queue_kind_b,
									  TimeLineID timeline_id_b,
									  XLogSegNo segno_b,
									  int recent_rank_b)
{
	if (queue_kind_a != queue_kind_b)
		return (queue_kind_a == FB_SUMMARY_SERVICE_QUEUE_HOT) ? 1 : -1;
	if (timeline_id_a < timeline_id_b)
		return -1;
	if (timeline_id_a > timeline_id_b)
		return 1;
	if (queue_kind_a == FB_SUMMARY_SERVICE_QUEUE_COLD)
	{
		if (segno_a < segno_b)
			return 1;
		if (segno_a > segno_b)
			return -1;
	}
	else
	{
		if (segno_a < segno_b)
			return -1;
		if (segno_a > segno_b)
			return 1;
	}
	if (recent_rank_a > recent_rank_b)
		return -1;
	if (recent_rank_a < recent_rank_b)
		return 1;
	return 0;
}

static bool
fb_summary_service_worker_pid_known_locked(pid_t owner_pid)
{
	int i;

	if (owner_pid == 0)
		return false;
	for (i = 0; i < lengthof(fb_summary_service->worker_pids); i++)
	{
		if (fb_summary_service->worker_pids[i] == owner_pid)
			return true;
	}
	return false;
}

static void
fb_summary_service_recover_stale_tasks_locked(void)
{
	int i;

	for (i = 0; i < fb_summary_service->queue_capacity; i++)
	{
		FbSummaryServiceTask *task = &fb_summary_service->tasks[i];

		if (task->state != FB_SUMMARY_SERVICE_TASK_RUNNING)
			continue;
		if (fb_summary_service_worker_pid_known_locked(task->owner_pid))
			continue;
		task->state = FB_SUMMARY_SERVICE_TASK_PENDING;
		task->owner_pid = 0;
		task->lease_started_at = 0;
	}
}

static void
fb_summary_service_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	fb_summary_service_got_sigterm = true;
	if (MyLatch != NULL)
		SetLatch(MyLatch);
	errno = save_errno;
}

static bool
fb_summary_service_candidate_summary_exists_fast(const FbSummaryBuildCandidate *candidate)
{
	char summary_path[MAXPGPATH];
	struct stat st;
	uint64 file_identity_hash;
	int written;

	if (candidate == NULL)
		return false;

	file_identity_hash = fb_summary_candidate_identity_hash(candidate);
	if (file_identity_hash == 0)
		return false;

	written = snprintf(summary_path,
					   sizeof(summary_path),
					   "%s/pg_flashback/meta/summary/summary-%016llx.meta",
					   DataDir,
					   (unsigned long long) file_identity_hash);
	if (written < 0 || written >= (int) sizeof(summary_path))
		return false;

	return stat(summary_path, &st) == 0;
}

static bool
fb_summary_service_candidate_summary_exists_for_enqueue(const FbSummaryBuildCandidate *candidate)
{
	if (!fb_summary_service_candidate_summary_exists_fast(candidate))
		return false;

	return fb_summary_candidate_summary_exists(candidate);
}

static int
fb_summary_service_find_best_task_slot_locked(int queue_kind)
{
	int i;
	int best_slot = -1;
	FbSummaryServiceTask *best_task = NULL;

	for (i = 0; i < fb_summary_service->queue_capacity; i++)
	{
		FbSummaryServiceTask *task = &fb_summary_service->tasks[i];

		if (task->state != FB_SUMMARY_SERVICE_TASK_PENDING)
			continue;
		if (queue_kind != 0 && task->queue_kind != queue_kind)
			continue;
		if (best_task == NULL ||
			fb_summary_service_compare_priority(task->queue_kind,
											   task->candidate.timeline_id,
											   task->candidate.segno,
											   task->recent_rank,
											   best_task->queue_kind,
											   best_task->candidate.timeline_id,
											   best_task->candidate.segno,
											   best_task->recent_rank) > 0)
		{
			best_slot = i;
			best_task = task;
		}
	}

	return best_slot;
}

static int
fb_summary_service_find_contiguous_cold_slot_locked(TimeLineID timeline_id,
													 XLogSegNo segno)
{
	int i;

	for (i = 0; i < fb_summary_service->queue_capacity; i++)
	{
		FbSummaryServiceTask *task = &fb_summary_service->tasks[i];

		if (task->state != FB_SUMMARY_SERVICE_TASK_PENDING)
			continue;
		if (task->queue_kind != FB_SUMMARY_SERVICE_QUEUE_COLD)
			continue;
		if (task->candidate.timeline_id != timeline_id ||
			task->candidate.segno != segno)
			continue;
		return i;
	}

	return -1;
}

static void
fb_summary_service_mark_task_running_locked(FbSummaryServiceTask *task,
											 FbSummaryBuildCandidate *candidate_out,
											 int *slot_index_out,
											 int slot_index)
{
	task->state = FB_SUMMARY_SERVICE_TASK_RUNNING;
	task->owner_pid = MyProcPid;
	task->attempt_count++;
	task->lease_started_at = GetCurrentTimestamp();
	if (candidate_out != NULL)
		*candidate_out = task->candidate;
	if (slot_index_out != NULL)
		*slot_index_out = slot_index;
}

static FbSummaryServiceTask *
fb_summary_service_find_task_locked(uint64 file_identity_hash)
{
	int i;

	for (i = 0; i < fb_summary_service->queue_capacity; i++)
	{
		FbSummaryServiceTask *task = &fb_summary_service->tasks[i];

		if (task->state == FB_SUMMARY_SERVICE_TASK_EMPTY)
			continue;
		if (task->file_identity_hash == file_identity_hash)
			return task;
	}
	return NULL;
}

static bool
fb_summary_service_try_enqueue(const FbSummaryBuildCandidate *candidate,
								 int queue_kind,
								 int recent_rank)
{
	uint64 file_identity_hash;
	int i;
	bool queued = false;

	if (candidate == NULL)
		return false;
	file_identity_hash = fb_summary_candidate_identity_hash(candidate);
	if (file_identity_hash == 0)
		return false;
	if (fb_summary_service_candidate_summary_exists_for_enqueue(candidate))
		return false;

	LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
	{
		FbSummaryServiceTask *existing = fb_summary_service_find_task_locked(file_identity_hash);

		if (existing != NULL)
		{
			if (existing->state == FB_SUMMARY_SERVICE_TASK_PENDING &&
				fb_summary_service_compare_priority(queue_kind,
												   candidate->timeline_id,
												   candidate->segno,
												   recent_rank,
												   existing->queue_kind,
												   existing->candidate.timeline_id,
												   existing->candidate.segno,
												   existing->recent_rank) > 0)
			{
				existing->queue_kind = queue_kind;
				existing->recent_rank = recent_rank;
				existing->candidate = *candidate;
			}
			LWLockRelease(fb_summary_service_lock);
			return false;
		}
	}

	for (i = 0; i < fb_summary_service->queue_capacity; i++)
	{
		FbSummaryServiceTask *task = &fb_summary_service->tasks[i];

		if (task->state != FB_SUMMARY_SERVICE_TASK_EMPTY)
			continue;
		MemSet(task, 0, sizeof(*task));
		task->state = FB_SUMMARY_SERVICE_TASK_PENDING;
		task->queue_kind = queue_kind;
		task->recent_rank = recent_rank;
		task->file_identity_hash = file_identity_hash;
		task->candidate = *candidate;
		fb_summary_service->enqueue_count++;
		queued = true;
		break;
	}
	LWLockRelease(fb_summary_service_lock);
	return queued;
}

static int
fb_summary_service_claim_tasks(FbSummaryBuildCandidate *candidates_out,
							   int *slot_indexes_out,
							   int max_count,
							   int preferred_queue_kind,
							   bool allow_fallback_queue)
{
	int claim_count = 0;
	int slot_index = -1;
	int first_queue_kind = 0;
	TimeLineID batch_timeline_id = 0;
	XLogSegNo next_batch_segno = 0;

	LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
	fb_summary_service_recover_stale_tasks_locked();
	slot_index = fb_summary_service_find_best_task_slot_locked(preferred_queue_kind);
	if (slot_index < 0 && allow_fallback_queue)
	{
		int fallback_queue_kind =
			(preferred_queue_kind == FB_SUMMARY_SERVICE_QUEUE_HOT) ?
			FB_SUMMARY_SERVICE_QUEUE_COLD :
			FB_SUMMARY_SERVICE_QUEUE_HOT;

		slot_index = fb_summary_service_find_best_task_slot_locked(fallback_queue_kind);
	}
	if (slot_index >= 0)
	{
		FbSummaryServiceTask *task = &fb_summary_service->tasks[slot_index];

		fb_summary_service_mark_task_running_locked(task,
												   (candidates_out != NULL) ?
												   &candidates_out[claim_count] : NULL,
												   (slot_indexes_out != NULL) ?
												   &slot_indexes_out[claim_count] : NULL,
												   slot_index);
		first_queue_kind = task->queue_kind;
		batch_timeline_id = task->candidate.timeline_id;
		next_batch_segno = (first_queue_kind == FB_SUMMARY_SERVICE_QUEUE_COLD) ?
			task->candidate.segno + 1 :
			task->candidate.segno - 1;
		claim_count++;
	}
	while (claim_count < max_count &&
		   first_queue_kind == FB_SUMMARY_SERVICE_QUEUE_COLD)
	{
		FbSummaryServiceTask *task;

		slot_index = fb_summary_service_find_contiguous_cold_slot_locked(batch_timeline_id,
																	  next_batch_segno);
		if (slot_index < 0)
			break;
		task = &fb_summary_service->tasks[slot_index];
		fb_summary_service_mark_task_running_locked(task,
												   (candidates_out != NULL) ?
												   &candidates_out[claim_count] : NULL,
												   (slot_indexes_out != NULL) ?
												   &slot_indexes_out[claim_count] : NULL,
												   slot_index);
		claim_count++;
		next_batch_segno++;
	}
	LWLockRelease(fb_summary_service_lock);
	return claim_count;
}

static void
fb_summary_service_finish_task(int slot_index, bool built)
{
	if (slot_index < 0 || slot_index >= fb_summary_service->queue_capacity)
		return;

	LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
	if (built)
	{
		TimestampTz now = GetCurrentTimestamp();

		fb_summary_service->build_count++;
		if (fb_summary_service->throughput_window_started_at == 0 ||
			TimestampDifferenceExceeds(fb_summary_service->throughput_window_started_at,
									 now,
									 FB_SUMMARY_SERVICE_RATE_WINDOW_MS))
		{
			fb_summary_service->throughput_window_started_at = now;
			fb_summary_service->throughput_window_builds = 1;
		}
		else
			fb_summary_service->throughput_window_builds++;
		fb_summary_service->last_build_at = now;
	}
	MemSet(&fb_summary_service->tasks[slot_index], 0, sizeof(FbSummaryServiceTask));
	LWLockRelease(fb_summary_service_lock);
}

static void
fb_summary_service_run_scan(void)
{
	FbSummaryBuildCandidate *candidates = NULL;
	uint64 *live_hashes = NULL;
	int live_hash_count = 0;
	int candidate_count;
	int hot_window;
	int i;

	if (!fb_summary_service_enabled())
		return;

	candidate_count = fb_summary_collect_build_candidates(&candidates, false);
	fb_summary_service_collect_candidate_hashes(candidates,
												 candidate_count,
												 &live_hashes,
												 &live_hash_count);
	hot_window = fb_summary_service_hot_window();
	LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
	fb_summary_service_recover_stale_tasks_locked();
	fb_summary_service_prune_missing_pending_tasks_locked(live_hashes,
														 live_hash_count);
	fb_summary_service->scan_count++;
	fb_summary_service->last_scan_at = GetCurrentTimestamp();
	if (candidate_count > 0)
	{
		fb_summary_service->snapshot_valid = true;
		fb_summary_service->snapshot_timeline_id = candidates[candidate_count - 1].timeline_id;
		fb_summary_service->snapshot_oldest_segno = candidates[0].segno;
		fb_summary_service->snapshot_newest_segno = candidates[candidate_count - 1].segno;
		fb_summary_service->snapshot_stable_candidates = candidate_count;
		fb_summary_service->snapshot_hot_candidates = Min(hot_window, candidate_count);
	}
	else
	{
		fb_summary_service->snapshot_valid = false;
		fb_summary_service->snapshot_timeline_id = 0;
		fb_summary_service->snapshot_oldest_segno = 0;
		fb_summary_service->snapshot_newest_segno = 0;
		fb_summary_service->snapshot_stable_candidates = 0;
		fb_summary_service->snapshot_hot_candidates = 0;
	}
	LWLockRelease(fb_summary_service_lock);

	for (i = candidate_count - 1; i >= 0; i--)
	{
		int recent_rank = fb_summary_service_recent_rank_for_index(i, candidate_count);
		int queue_kind = fb_summary_service_queue_kind_for_rank(recent_rank, hot_window);

		if (queue_kind != FB_SUMMARY_SERVICE_QUEUE_HOT)
			continue;
		if (!fb_summary_service_try_enqueue(&candidates[i], queue_kind, recent_rank))
			continue;
	}

	for (i = 0; i < candidate_count; i++)
	{
		int recent_rank = fb_summary_service_recent_rank_for_index(i, candidate_count);
		int queue_kind = fb_summary_service_queue_kind_for_rank(recent_rank, hot_window);

		if (queue_kind != FB_SUMMARY_SERVICE_QUEUE_COLD)
			continue;
		if (!fb_summary_service_try_enqueue(&candidates[i], queue_kind, recent_rank))
			continue;
	}

	fb_summary_free_build_candidates(candidates);
	if (live_hashes != NULL)
		pfree(live_hashes);
}

static bool
fb_summary_service_build_candidate_safe(const FbSummaryBuildCandidate *candidate)
{
	MemoryContext build_mcxt;
	MemoryContext oldcontext;
	bool built = false;

	if (candidate == NULL)
		return false;

	build_mcxt = AllocSetContextCreate(TopMemoryContext,
									   "fb_summary_service_build_candidate",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(build_mcxt);
	PG_TRY();
	{
		built = fb_summary_build_candidate(candidate);
		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(build_mcxt);
	}
	PG_CATCH();
	{
		ErrorData *edata;

		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(build_mcxt);
		edata = CopyErrorData();
		FlushErrorState();
		elog(LOG,
			 "fb_summary_service skipped candidate timeline=%u segno=%llu path=\"%s\": %s",
			 candidate->timeline_id,
			 (unsigned long long) candidate->segno,
			 candidate->path,
			 (edata != NULL && edata->message != NULL) ? edata->message : "unknown error");
		if (edata != NULL)
			FreeErrorData(edata);
		built = false;
	}
	PG_END_TRY();

	return built;
}

typedef struct FbSummaryCleanupEntry
{
	char path[MAXPGPATH];
	uint64 file_identity_hash;
	uint64 size_bytes;
	time_t mtime_sec;
	long mtime_nsec;
} FbSummaryCleanupEntry;

static int
fb_summary_cleanup_entry_cmp(const void *lhs, const void *rhs)
{
	const FbSummaryCleanupEntry *left = (const FbSummaryCleanupEntry *) lhs;
	const FbSummaryCleanupEntry *right = (const FbSummaryCleanupEntry *) rhs;

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

static void
fb_summary_service_run_cleanup(void)
{
	char *summary_dir;
	DIR *dir;
	struct dirent *de;
	FbSummaryBuildCandidate *candidates = NULL;
	FbSummaryCleanupEntry *entries = NULL;
	uint64 *live_hashes = NULL;
	int live_hash_count = 0;
	int entry_count = 0;
	int entry_capacity = 0;
	uint64 total_bytes;
	uint64 low_bytes;
	time_t now_sec = time(NULL);
	int i;
	uint64 *protected_hashes = NULL;
	int protected_count = 0;
	int candidate_count;

	total_bytes = fb_summary_meta_summary_size_bytes(NULL);
	candidate_count = fb_summary_collect_build_candidates(&candidates, false);
	fb_summary_service_collect_candidate_hashes(candidates,
												 candidate_count,
												 &live_hashes,
												 &live_hash_count);

	low_bytes = (fb_summary_service_meta_limit_bytes() *
				 (uint64) fb_summary_service_meta_low_watermark_percent()) / 100;
	fb_summary_service_collect_protected_hashes(&protected_hashes, &protected_count, true);

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
		return;
	}

	while ((de = ReadDir(dir, summary_dir)) != NULL)
	{
		char path[MAXPGPATH];
		struct stat st;
		uint64 parsed_hash = 0;
		bool protected = false;

		if (!fb_summary_service_parse_summary_filename(de->d_name, &parsed_hash))
			continue;
		snprintf(path, sizeof(path), "%s/%s", summary_dir, de->d_name);
		if (stat(path, &st) != 0)
			continue;
		if ((now_sec - st.st_mtime) < FB_SUMMARY_SERVICE_RECENT_PROTECT_SECS)
			continue;
		if (!fb_summary_service_hash_in_list(live_hashes,
											 live_hash_count,
											 (uint64) parsed_hash))
		{
			if (unlink(path) == 0)
			{
				total_bytes -= Min(total_bytes, (uint64) st.st_size);
				LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
				fb_summary_service->cleanup_count++;
				LWLockRelease(fb_summary_service_lock);
			}
			continue;
		}
		protected = fb_summary_service_hash_in_list(protected_hashes,
													 protected_count,
													 parsed_hash);
		if (protected)
			continue;

		if (entry_count == entry_capacity)
		{
			entry_capacity = (entry_capacity == 0) ? 64 : (entry_capacity * 2);
			if (entries == NULL)
				entries = palloc(sizeof(FbSummaryCleanupEntry) * entry_capacity);
			else
				entries = repalloc(entries, sizeof(FbSummaryCleanupEntry) * entry_capacity);
		}

		strlcpy(entries[entry_count].path, path, sizeof(entries[entry_count].path));
		entries[entry_count].file_identity_hash = parsed_hash;
		entries[entry_count].size_bytes = (uint64) st.st_size;
		entries[entry_count].mtime_sec = st.st_mtime;
#if defined(__APPLE__)
		entries[entry_count].mtime_nsec = st.st_mtimespec.tv_nsec;
#else
		entries[entry_count].mtime_nsec = st.st_mtim.tv_nsec;
#endif
		entry_count++;
	}
	FreeDir(dir);
	pfree(summary_dir);
	fb_summary_free_build_candidates(candidates);
	if (live_hashes != NULL)
		pfree(live_hashes);

	if (total_bytes <= fb_summary_service_meta_limit_bytes())
	{
		if (entries != NULL)
			pfree(entries);
		if (protected_hashes != NULL)
			pfree(protected_hashes);
		return;
	}

	if (entry_count == 0)
	{
		if (protected_hashes != NULL)
			pfree(protected_hashes);
		return;
	}

	qsort(entries, entry_count, sizeof(FbSummaryCleanupEntry), fb_summary_cleanup_entry_cmp);
	for (i = 0; i < entry_count && total_bytes > low_bytes; i++)
	{
		if (unlink(entries[i].path) != 0)
			continue;
		total_bytes -= Min(total_bytes, entries[i].size_bytes);
		LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
		fb_summary_service->cleanup_count++;
		LWLockRelease(fb_summary_service_lock);
	}

	pfree(entries);
	if (protected_hashes != NULL)
		pfree(protected_hashes);
}

static void
fb_summary_service_collect_progress(FbSummaryServiceProgressStats *stats)
{
	FbSummaryBuildCandidate *candidates = NULL;
	bool *summary_exists = NULL;
	TimestampTz *oldest_xact_ts = NULL;
	TimestampTz *newest_xact_ts = NULL;
	XLogSegNo *filtered_segnos = NULL;
	bool snapshot_valid = false;
	bool use_snapshot_filter = false;
	int candidate_count;
	int filtered_count = 0;
	int filtered_index = 0;
	int i;

	MemSet(stats, 0, sizeof(*stats));
	stats->service_enabled = fb_summary_service_enabled();
	stats->queue_capacity = fb_summary_service_queue_size();
	stats->registered_workers = fb_summary_service_registered_workers();
	stats->hot_window = fb_summary_service_hot_window();
	stats->summary_bytes = fb_summary_meta_summary_size_bytes(&stats->summary_files);

	if (fb_summary_service != NULL)
	{
		LWLockAcquire(fb_summary_service_lock, LW_SHARED);
			stats->queue_capacity = fb_summary_service->queue_capacity;
			stats->launcher_pid = fb_summary_service->launcher_pid;
			snapshot_valid = fb_summary_service->snapshot_valid;
			stats->snapshot_timeline_id = fb_summary_service->snapshot_timeline_id;
			stats->snapshot_oldest_segno = fb_summary_service->snapshot_oldest_segno;
			stats->snapshot_newest_segno = fb_summary_service->snapshot_newest_segno;
			stats->snapshot_hot_candidates = fb_summary_service->snapshot_hot_candidates;
			stats->scan_count = fb_summary_service->scan_count;
			stats->enqueue_count = fb_summary_service->enqueue_count;
			stats->build_count = fb_summary_service->build_count;
			stats->cleanup_count = fb_summary_service->cleanup_count;
			stats->last_scan_at = fb_summary_service->last_scan_at;
			stats->throughput_window_started_at =
				fb_summary_service->throughput_window_started_at;
			stats->last_build_at = fb_summary_service->last_build_at;
			stats->throughput_window_builds =
				fb_summary_service->throughput_window_builds;
			stats->last_query_observed_at = fb_summary_service->last_query_observed_at;
			stats->last_query_summary_span_fallback_segments =
				fb_summary_service->last_query_summary_span_fallback_segments;
			stats->last_query_metadata_fallback_segments =
				fb_summary_service->last_query_metadata_fallback_segments;
		for (i = 0; i < lengthof(fb_summary_service->worker_pids); i++)
		{
			if (fb_summary_service->worker_pids[i] != 0)
				stats->active_workers++;
		}
		for (i = 0; i < fb_summary_service->queue_capacity; i++)
		{
			FbSummaryServiceTask *task = &fb_summary_service->tasks[i];
			bool running_owner_known;

			if (task->state == FB_SUMMARY_SERVICE_TASK_PENDING)
			{
				if (task->queue_kind == FB_SUMMARY_SERVICE_QUEUE_HOT)
					stats->pending_hot++;
				else
					stats->pending_cold++;
			}
			else if (task->state == FB_SUMMARY_SERVICE_TASK_RUNNING)
			{
				running_owner_known =
					fb_summary_service_worker_pid_known_locked(task->owner_pid);
				if (task->queue_kind == FB_SUMMARY_SERVICE_QUEUE_HOT)
				{
					if (running_owner_known)
						stats->running_hot++;
					else
						stats->pending_hot++;
				}
				else
				{
					if (running_owner_known)
						stats->running_cold++;
					else
						stats->pending_cold++;
				}
			}
		}
		LWLockRelease(fb_summary_service_lock);
	}
	stats->last_query_summary_ready =
		(stats->last_query_observed_at != 0 &&
		 stats->last_query_summary_span_fallback_segments == 0 &&
		 stats->last_query_metadata_fallback_segments == 0);

	candidate_count = fb_summary_collect_build_candidates(&candidates, false);
	use_snapshot_filter = snapshot_valid;
	for (i = 0; i < candidate_count; i++)
	{
		if (use_snapshot_filter)
		{
			if (candidates[i].timeline_id != stats->snapshot_timeline_id)
				continue;
			if (candidates[i].segno < stats->snapshot_oldest_segno ||
				candidates[i].segno > stats->snapshot_newest_segno)
				continue;
		}
		filtered_count++;
	}
	if (use_snapshot_filter && filtered_count == 0 && candidate_count > 0)
	{
		use_snapshot_filter = false;
		filtered_count = candidate_count;
	}
	if (filtered_count > 0)
	{
		summary_exists = palloc0(sizeof(bool) * filtered_count);
		oldest_xact_ts = palloc0(sizeof(TimestampTz) * filtered_count);
		newest_xact_ts = palloc0(sizeof(TimestampTz) * filtered_count);
		filtered_segnos = palloc0(sizeof(XLogSegNo) * filtered_count);
	}
	if (use_snapshot_filter)
		stats->snapshot_hot_candidates = Min(stats->snapshot_hot_candidates, filtered_count);
	else
		stats->snapshot_hot_candidates = Min(stats->hot_window, filtered_count);
	stats->snapshot_cold_candidates =
		Max(0, filtered_count - stats->snapshot_hot_candidates);
	for (i = 0; i < candidate_count; i++)
	{
		int recent_rank;
		int queue_kind;

		if (use_snapshot_filter)
		{
			if (candidates[i].timeline_id != stats->snapshot_timeline_id)
				continue;
			if (candidates[i].segno < stats->snapshot_oldest_segno ||
				candidates[i].segno > stats->snapshot_newest_segno)
				continue;
		}

		recent_rank = fb_summary_service_recent_rank_for_index(filtered_index, filtered_count);
		queue_kind = (recent_rank <= stats->snapshot_hot_candidates) ?
			FB_SUMMARY_SERVICE_QUEUE_HOT :
			FB_SUMMARY_SERVICE_QUEUE_COLD;
		filtered_segnos[filtered_index] = candidates[i].segno;
		summary_exists[filtered_index] =
			fb_summary_candidate_time_bounds(&candidates[i],
												 &oldest_xact_ts[filtered_index],
												 &newest_xact_ts[filtered_index]);
		if (summary_exists[filtered_index])
			stats->completed_summaries++;
		else
		{
			stats->missing_summaries++;
			if (queue_kind == FB_SUMMARY_SERVICE_QUEUE_HOT)
				stats->hot_missing_summaries++;
				else
					stats->cold_missing_summaries++;
		}
		filtered_index++;
	}
	if (filtered_count > 0)
	{
		stats->visible_oldest_segno = filtered_segnos[0];
		stats->visible_newest_segno = filtered_segnos[filtered_count - 1];
		stats->stable_candidates =
			(uint64) (stats->visible_newest_segno - stats->visible_oldest_segno) + 1;
	}
	for (i = 0; i < filtered_count; i++)
	{
		if (oldest_xact_ts[i] != 0)
		{
			stats->stable_oldest_ts = oldest_xact_ts[i];
			break;
		}
	}
	for (i = filtered_count - 1; i >= 0; i--)
	{
		if (newest_xact_ts[i] != 0)
		{
			stats->stable_newest_ts = newest_xact_ts[i];
			break;
		}
	}
	for (i = filtered_count - 1; i >= 0; i--)
	{
		if (i + 1 < filtered_count &&
			filtered_segnos[i] + 1 < filtered_segnos[i + 1])
		{
			stats->first_gap_from_newest_segno = filtered_segnos[i + 1] - 1;
			if (oldest_xact_ts[i + 1] != 0)
				stats->first_gap_from_newest_ts = oldest_xact_ts[i + 1];
			break;
		}
		if (!summary_exists[i])
		{
			stats->first_gap_from_newest_segno = filtered_segnos[i];
			if (i + 1 < filtered_count && oldest_xact_ts[i + 1] != 0)
				stats->first_gap_from_newest_ts = oldest_xact_ts[i + 1];
			break;
		}
		if (oldest_xact_ts[i] != 0 &&
			(stats->near_contiguous_through_ts == 0 ||
			 oldest_xact_ts[i] < stats->near_contiguous_through_ts))
				stats->near_contiguous_through_ts = oldest_xact_ts[i];
	}
	for (i = 0; i < filtered_count; i++)
	{
		if (i > 0 && filtered_segnos[i - 1] + 1 < filtered_segnos[i])
		{
			stats->first_gap_from_oldest_segno = filtered_segnos[i - 1] + 1;
			if (newest_xact_ts[i - 1] != 0)
				stats->first_gap_from_oldest_ts = newest_xact_ts[i - 1];
			break;
		}
		if (!summary_exists[i])
		{
			stats->first_gap_from_oldest_segno = filtered_segnos[i];
			if (i > 0 && newest_xact_ts[i - 1] != 0)
				stats->first_gap_from_oldest_ts = newest_xact_ts[i - 1];
			break;
		}
		if (newest_xact_ts[i] != 0 &&
			(stats->far_contiguous_until_ts == 0 ||
			 newest_xact_ts[i] > stats->far_contiguous_until_ts))
			stats->far_contiguous_until_ts = newest_xact_ts[i];
	}
	for (i = 1; i < filtered_count; i++)
	{
		if (filtered_segnos[i - 1] + 1 < filtered_segnos[i])
			stats->missing_summaries +=
				(uint64) (filtered_segnos[i] - filtered_segnos[i - 1] - 1);
	}
	if (summary_exists != NULL)
		pfree(summary_exists);
	if (oldest_xact_ts != NULL)
		pfree(oldest_xact_ts);
	if (newest_xact_ts != NULL)
		pfree(newest_xact_ts);
	if (filtered_segnos != NULL)
		pfree(filtered_segnos);
	fb_summary_free_build_candidates(candidates);
	fb_summary_service_compute_eta(stats);
}

static void
fb_summary_service_compute_eta(FbSummaryServiceProgressStats *stats)
{
	long secs = 0;
	int microsecs = 0;
	double elapsed_secs;
	double builds_per_sec;
	double remaining_usecs;
	TimestampTz sample_end_at;
	TimestampTz now;
	int64 eta_offset;
	int64 eta_at;

	if (stats == NULL ||
		!stats->service_enabled ||
		stats->missing_summaries == 0 ||
		stats->throughput_window_started_at == 0 ||
		stats->throughput_window_builds == 0)
		return;

	sample_end_at = stats->last_build_at;
	if (stats->last_scan_at != 0 &&
		(sample_end_at == 0 || stats->last_scan_at > sample_end_at))
		sample_end_at = stats->last_scan_at;
	if (sample_end_at == 0)
		return;

	TimestampDifference(stats->throughput_window_started_at,
						sample_end_at,
						&secs,
						&microsecs);
	elapsed_secs = (double) secs + ((double) microsecs / 1000000.0);
	if (elapsed_secs < ((double) FB_SUMMARY_SERVICE_RATE_MIN_SAMPLE_MS / 1000.0))
		return;

	builds_per_sec = (double) stats->throughput_window_builds / elapsed_secs;
	if (builds_per_sec <= 0.0)
		return;

	remaining_usecs =
		((double) stats->missing_summaries / builds_per_sec) * 1000000.0;
	if (remaining_usecs <= 0.0 ||
		remaining_usecs > (double) PG_INT64_MAX)
		return;

	now = GetCurrentTimestamp();
	eta_offset = (int64) rint(remaining_usecs);
	if (pg_add_s64_overflow(now, eta_offset, &eta_at))
		return;

	stats->estimated_completion_at = eta_at;
}

static bool
fb_summary_service_candidate_in_snapshot(bool snapshot_valid,
										 TimeLineID snapshot_timeline_id,
										 XLogSegNo snapshot_oldest_segno,
										 XLogSegNo snapshot_newest_segno,
										 const FbSummaryBuildCandidate *candidate)
{
	if (candidate == NULL)
		return false;
	if (!snapshot_valid)
		return true;
	if (candidate->timeline_id != snapshot_timeline_id)
		return false;
	if (candidate->segno < snapshot_oldest_segno ||
		candidate->segno > snapshot_newest_segno)
		return false;
	return true;
}

static bool
fb_summary_service_hash_in_list(const uint64 *hashes, int hash_count, uint64 target_hash)
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

static bool
fb_summary_service_parse_summary_filename(const char *name, uint64 *hash_out)
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

static void
fb_summary_service_collect_candidate_hashes(const FbSummaryBuildCandidate *candidates,
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

		if (hash == 0)
			continue;
		if (fb_summary_service_hash_in_list(hashes, hash_count, hash))
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

static void
fb_summary_service_prune_missing_pending_tasks_locked(const uint64 *live_hashes,
														 int live_count)
{
	int i;

	if (fb_summary_service == NULL)
		return;

	for (i = 0; i < fb_summary_service->queue_capacity; i++)
	{
		FbSummaryServiceTask *task = &fb_summary_service->tasks[i];

		if (task->state != FB_SUMMARY_SERVICE_TASK_PENDING ||
			task->file_identity_hash == 0)
			continue;
		if (fb_summary_service_hash_in_list(live_hashes,
											 live_count,
											 task->file_identity_hash))
			continue;
		MemSet(task, 0, sizeof(*task));
	}
}

static void
fb_summary_service_collect_protected_hashes(uint64 **hashes_out,
											 int *count_out,
											 bool include_snapshot)
{
	uint64 *hashes = NULL;
	int hash_count = 0;
	int hash_capacity = 0;
	bool snapshot_valid = false;
	TimeLineID snapshot_timeline_id = 0;
	XLogSegNo snapshot_oldest_segno = 0;
	XLogSegNo snapshot_newest_segno = 0;
	FbSummaryBuildCandidate *candidates = NULL;
	int candidate_count;
	int i;

	if (hashes_out != NULL)
		*hashes_out = NULL;
	if (count_out != NULL)
		*count_out = 0;

	LWLockAcquire(fb_summary_service_lock, LW_SHARED);
	if (fb_summary_service != NULL)
	{
		snapshot_valid = fb_summary_service->snapshot_valid;
		snapshot_timeline_id = fb_summary_service->snapshot_timeline_id;
		snapshot_oldest_segno = fb_summary_service->snapshot_oldest_segno;
		snapshot_newest_segno = fb_summary_service->snapshot_newest_segno;
		hash_capacity = fb_summary_service->queue_capacity + 128;
		hashes = palloc(sizeof(uint64) * hash_capacity);
		for (i = 0; i < fb_summary_service->queue_capacity; i++)
		{
			FbSummaryServiceTask *task = &fb_summary_service->tasks[i];

			if (task->state == FB_SUMMARY_SERVICE_TASK_EMPTY ||
				task->file_identity_hash == 0)
				continue;
			if (fb_summary_service_hash_in_list(hashes, hash_count, task->file_identity_hash))
				continue;
			hashes[hash_count++] = task->file_identity_hash;
		}
	}
	LWLockRelease(fb_summary_service_lock);

	if (!include_snapshot)
	{
		if (hashes_out != NULL)
			*hashes_out = hashes;
		else if (hashes != NULL)
			pfree(hashes);
		if (count_out != NULL)
			*count_out = hash_count;
		return;
	}

	candidate_count = fb_summary_collect_build_candidates(&candidates, true);
	for (i = 0; i < candidate_count; i++)
	{
		uint64 hash;

		if (!fb_summary_service_candidate_in_snapshot(snapshot_valid,
													 snapshot_timeline_id,
													 snapshot_oldest_segno,
													 snapshot_newest_segno,
													 &candidates[i]))
			continue;
		hash = fb_summary_candidate_identity_hash(&candidates[i]);
		if (hash == 0)
			continue;
		if (hash_count >= hash_capacity)
		{
			hash_capacity = (hash_capacity == 0) ? 128 : (hash_capacity * 2);
			hashes = (hashes == NULL) ?
				palloc(sizeof(uint64) * hash_capacity) :
				repalloc(hashes, sizeof(uint64) * hash_capacity);
		}
		if (fb_summary_service_hash_in_list(hashes, hash_count, hash))
			continue;
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

void
fb_summary_service_launcher_main(Datum main_arg)
{
	(void) main_arg;

	pqsignal(SIGTERM, fb_summary_service_sigterm);
	BackgroundWorkerUnblockSignals();

	LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
	fb_summary_service->launcher_pid = MyProcPid;
	LWLockRelease(fb_summary_service_lock);

	while (!fb_summary_service_got_sigterm)
	{
		int rc;

		fb_summary_service_run_scan();
		fb_summary_service_run_cleanup();

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   fb_summary_service_scan_interval_ms(),
					   0);
		ResetLatch(MyLatch);
		if (rc & WL_EXIT_ON_PM_DEATH)
			proc_exit(1);
	}

	proc_exit(0);
}

void
fb_summary_service_worker_main(Datum main_arg)
{
	FbSummaryBuildCandidate candidates[FB_SUMMARY_SERVICE_COLD_BATCH_SIZE];
	int slot_indexes[FB_SUMMARY_SERVICE_COLD_BATCH_SIZE];
	int worker_index = DatumGetInt32(main_arg);
	int preferred_queue_kind =
		(worker_index == 0) ?
		FB_SUMMARY_SERVICE_QUEUE_HOT :
		FB_SUMMARY_SERVICE_QUEUE_COLD;

	pqsignal(SIGTERM, fb_summary_service_sigterm);
	BackgroundWorkerUnblockSignals();

	if (worker_index >= 0 && worker_index < 8)
	{
		LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
		fb_summary_service->worker_pids[worker_index] = MyProcPid;
		LWLockRelease(fb_summary_service_lock);
	}

	while (!fb_summary_service_got_sigterm)
	{
		int claimed_count;
		int rc;

		claimed_count = fb_summary_service_claim_tasks(candidates,
													  slot_indexes,
													  FB_SUMMARY_SERVICE_COLD_BATCH_SIZE,
													  preferred_queue_kind,
													  true);
		if (claimed_count > 0)
		{
			int i;

			for (i = 0; i < claimed_count; i++)
			{
				bool built = false;

				if (!fb_summary_candidate_summary_exists(&candidates[i]))
					built = fb_summary_service_build_candidate_safe(&candidates[i]);
				fb_summary_service_finish_task(slot_indexes[i], built);
			}
			continue;
		}

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   Min(fb_summary_service_scan_interval_ms(), 1000),
					   0);
		ResetLatch(MyLatch);
		if (rc & WL_EXIT_ON_PM_DEATH)
			proc_exit(1);
	}

	proc_exit(0);
}

Datum
fb_summary_progress_internal(PG_FUNCTION_ARGS)
{
	FbSummaryServiceProgressStats stats;
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[20];
	bool nulls[20];
	double progress_pct;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fb_summary_progress_internal must return a composite type")));

	tupdesc = BlessTupleDesc(tupdesc);
	MemSet(nulls, 0, sizeof(nulls));

	fb_summary_service_collect_progress(&stats);
	if (stats.stable_candidates == 0)
		progress_pct = 100.0;
	else
		progress_pct = ((double) stats.completed_summaries * 100.0) /
			(double) stats.stable_candidates;

	values[0] = BoolGetDatum(stats.service_enabled);
	if (stats.snapshot_timeline_id == 0)
		nulls[1] = true;
	else
		values[1] = Int32GetDatum((int32) stats.snapshot_timeline_id);
	if (stats.visible_oldest_segno == 0)
		nulls[2] = true;
	else
		values[2] = Int64GetDatum((int64) stats.visible_oldest_segno);
	if (stats.visible_newest_segno == 0)
		nulls[3] = true;
	else
		values[3] = Int64GetDatum((int64) stats.visible_newest_segno);
	if (stats.stable_oldest_ts == 0)
		nulls[4] = true;
	else
		values[4] = TimestampTzGetDatum(stats.stable_oldest_ts);
	if (stats.stable_newest_ts == 0)
		nulls[5] = true;
	else
		values[5] = TimestampTzGetDatum(stats.stable_newest_ts);
	if (stats.near_contiguous_through_ts == 0)
		nulls[6] = true;
	else
		values[6] = TimestampTzGetDatum(stats.near_contiguous_through_ts);
	if (stats.far_contiguous_until_ts == 0)
		nulls[7] = true;
	else
		values[7] = TimestampTzGetDatum(stats.far_contiguous_until_ts);
	if (stats.first_gap_from_newest_segno == 0)
		nulls[8] = true;
	else
		values[8] = Int64GetDatum((int64) stats.first_gap_from_newest_segno);
	if (stats.first_gap_from_newest_ts == 0)
		nulls[9] = true;
	else
		values[9] = TimestampTzGetDatum(stats.first_gap_from_newest_ts);
	if (stats.first_gap_from_oldest_segno == 0)
		nulls[10] = true;
	else
		values[10] = Int64GetDatum((int64) stats.first_gap_from_oldest_segno);
	if (stats.first_gap_from_oldest_ts == 0)
		nulls[11] = true;
	else
		values[11] = TimestampTzGetDatum(stats.first_gap_from_oldest_ts);
	values[12] = Int64GetDatum((int64) stats.completed_summaries);
	values[13] = Int64GetDatum((int64) stats.missing_summaries);
	values[14] = Float8GetDatum(progress_pct);
	if (stats.last_query_observed_at == 0)
		nulls[15] = true;
	else
		values[15] = TimestampTzGetDatum(stats.last_query_observed_at);
	if (stats.last_query_observed_at == 0)
		nulls[16] = true;
	else
		values[16] = BoolGetDatum(stats.last_query_summary_ready);
	if (stats.last_query_observed_at == 0)
		nulls[17] = true;
	else
		values[17] = Int64GetDatum((int64) stats.last_query_summary_span_fallback_segments);
	if (stats.last_query_observed_at == 0)
		nulls[18] = true;
	else
		values[18] = Int64GetDatum((int64) stats.last_query_metadata_fallback_segments);
	if (stats.estimated_completion_at == 0)
		nulls[19] = true;
	else
		values[19] = TimestampTzGetDatum(stats.estimated_completion_at);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum
fb_summary_service_debug_internal(PG_FUNCTION_ARGS)
{
	FbSummaryServiceProgressStats stats;
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[27];
	bool nulls[27];

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fb_summary_service_debug_internal must return a composite type")));

	tupdesc = BlessTupleDesc(tupdesc);
	MemSet(nulls, 0, sizeof(nulls));

	fb_summary_service_collect_progress(&stats);

	values[0] = BoolGetDatum(stats.service_enabled);
	if (stats.launcher_pid == 0)
		nulls[1] = true;
	else
		values[1] = Int32GetDatum((int32) stats.launcher_pid);
	values[2] = Int32GetDatum(stats.registered_workers);
	values[3] = Int32GetDatum(stats.active_workers);
	values[4] = Int32GetDatum(stats.queue_capacity);
	values[5] = Int32GetDatum(stats.hot_window);
	values[6] = Int32GetDatum(stats.pending_hot);
	values[7] = Int32GetDatum(stats.pending_cold);
	values[8] = Int32GetDatum(stats.running_hot);
	values[9] = Int32GetDatum(stats.running_cold);
	if (stats.snapshot_timeline_id == 0)
		nulls[10] = true;
	else
		values[10] = Int32GetDatum((int32) stats.snapshot_timeline_id);
	if (stats.snapshot_oldest_segno == 0)
		nulls[11] = true;
	else
		values[11] = Int64GetDatum((int64) stats.snapshot_oldest_segno);
	if (stats.snapshot_newest_segno == 0)
		nulls[12] = true;
	else
		values[12] = Int64GetDatum((int64) stats.snapshot_newest_segno);
	values[13] = Int32GetDatum(stats.snapshot_hot_candidates);
	values[14] = Int32GetDatum(stats.snapshot_cold_candidates);
	values[15] = Int64GetDatum((int64) stats.stable_candidates);
	values[16] = Int64GetDatum((int64) stats.completed_summaries);
	values[17] = Int64GetDatum((int64) stats.missing_summaries);
	values[18] = Int64GetDatum((int64) stats.hot_missing_summaries);
	values[19] = Int64GetDatum((int64) stats.cold_missing_summaries);
	values[20] = Int64GetDatum((int64) stats.summary_files);
	values[21] = Int64GetDatum((int64) stats.summary_bytes);
	values[22] = Int64GetDatum((int64) stats.scan_count);
	values[23] = Int64GetDatum((int64) stats.enqueue_count);
	values[24] = Int64GetDatum((int64) stats.build_count);
	values[25] = Int64GetDatum((int64) stats.cleanup_count);
	if (stats.last_scan_at == 0)
		nulls[26] = true;
	else
		values[26] = TimestampTzGetDatum(stats.last_scan_at);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum
fb_summary_service_plan_debug(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	FbSummaryServicePlanDebugContext *ctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		ctx = palloc0(sizeof(*ctx));
		ctx->candidate_count = fb_summary_collect_build_candidates(&ctx->candidates, true);
		ctx->hot_window = fb_summary_service_hot_window();
		if (ctx->candidate_count > 0)
		{
			int queue_capacity = fb_summary_service_queue_size();
			int enqueue_limit;
			int enqueue_count = 0;
			int i;

			if (fb_summary_service != NULL && fb_summary_service->queue_capacity > 0)
				queue_capacity = fb_summary_service->queue_capacity;
			enqueue_limit = Min(ctx->candidate_count, queue_capacity);

			ctx->enqueued = palloc0(sizeof(bool) * ctx->candidate_count);
			ctx->enqueue_order = palloc0(sizeof(int) * ctx->candidate_count);

			for (i = ctx->candidate_count - 1;
				 i >= 0 && enqueue_count < enqueue_limit;
				 i--)
			{
				int recent_rank = fb_summary_service_recent_rank_for_index(i,
																		  ctx->candidate_count);
				int queue_kind = fb_summary_service_queue_kind_for_rank(recent_rank,
																		 ctx->hot_window);

				if (queue_kind != FB_SUMMARY_SERVICE_QUEUE_HOT)
					continue;
				ctx->enqueued[i] = true;
				ctx->enqueue_order[i] = ++enqueue_count;
			}
			for (i = 0; i < ctx->candidate_count && enqueue_count < enqueue_limit; i++)
			{
				int recent_rank = fb_summary_service_recent_rank_for_index(i,
																		  ctx->candidate_count);
				int queue_kind = fb_summary_service_queue_kind_for_rank(recent_rank,
																		 ctx->hot_window);

				if (queue_kind != FB_SUMMARY_SERVICE_QUEUE_COLD)
					continue;
				ctx->enqueued[i] = true;
				ctx->enqueue_order[i] = ++enqueue_count;
			}
		}
		ctx->index = 0;
		funcctx->user_fctx = ctx;
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("fb_summary_service_plan_debug must return a composite type")));
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	ctx = (FbSummaryServicePlanDebugContext *) funcctx->user_fctx;

	while (ctx->index < ctx->candidate_count)
	{
		int candidate_index = ctx->index++;
		FbSummaryBuildCandidate *candidate = &ctx->candidates[candidate_index];
		int recent_rank = fb_summary_service_recent_rank_for_index(candidate_index,
																   ctx->candidate_count);
		int queue_kind = fb_summary_service_queue_kind_for_rank(recent_rank, ctx->hot_window);
		Datum values[8];
		bool nulls[8];
		HeapTuple tuple;

		MemSet(nulls, 0, sizeof(nulls));
		values[0] = Int32GetDatum(recent_rank);
		values[1] = Int32GetDatum(ctx->hot_window);
		values[2] = CStringGetTextDatum(fb_summary_service_queue_kind_name(queue_kind));
		values[3] = Int32GetDatum((int32) candidate->timeline_id);
		values[4] = Int64GetDatum((int64) candidate->segno);
		values[5] = BoolGetDatum(fb_summary_candidate_summary_exists(candidate));
		values[6] = BoolGetDatum(ctx->enqueued != NULL && ctx->enqueued[candidate_index]);
		if (ctx->enqueue_order == NULL || ctx->enqueue_order[candidate_index] == 0)
			nulls[7] = true;
		else
			values[7] = Int32GetDatum(ctx->enqueue_order[candidate_index]);
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	if (ctx->candidates != NULL)
	{
		fb_summary_free_build_candidates(ctx->candidates);
		ctx->candidates = NULL;
	}
	SRF_RETURN_DONE(funcctx);
}

Datum
fb_summary_service_worker_error_isolation_debug(PG_FUNCTION_ARGS)
{
	FbSummaryBuildCandidate *candidates = NULL;
	FbSummaryBuildCandidate candidate;
	char *runtime_dir;
	char bogus_path[MAXPGPATH];
	int candidate_count;
	bool isolated;

	fb_runtime_ensure_initialized();
	candidate_count = fb_summary_collect_build_candidates(&candidates, false);
	if (candidate_count <= 0)
		PG_RETURN_TEXT_P(cstring_to_text("worker_error_isolated=no-candidate"));

	runtime_dir = fb_runtime_runtime_dir();
	snprintf(bogus_path, sizeof(bogus_path), "%s/summary-worker-bogus-%d.wal",
			 runtime_dir, MyProcPid);
	pfree(runtime_dir);

	if (mkdir(bogus_path, S_IRWXU) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create bogus WAL directory \"%s\": %m",
						bogus_path)));

	MemSet(&candidate, 0, sizeof(candidate));
	strlcpy(candidate.path, bogus_path, sizeof(candidate.path));
	candidate.timeline_id = candidates[0].timeline_id;
	candidate.segno = candidates[0].segno;
	candidate.bytes = candidates[0].bytes;
	candidate.wal_seg_size = candidates[0].wal_seg_size;
	candidate.source_kind = candidates[0].source_kind;

	isolated = !fb_summary_service_build_candidate_safe(&candidate);

	rmdir(bogus_path);
	fb_summary_free_build_candidates(candidates);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf("worker_error_isolated=%s",
											  isolated ? "true" : "false")));
}

Datum
fb_summary_service_memory_reset_debug(PG_FUNCTION_ARGS)
{
	FbSummaryBuildCandidate *candidates = NULL;
	int32 repeat_count = PG_GETARG_INT32(0);
	Size before_bytes;
	Size after_bytes;
	bool reset_ok;
	int candidate_count;
	int i;

	if (repeat_count <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("repeat count must be positive")));

	fb_runtime_ensure_initialized();
	candidate_count = fb_summary_collect_build_candidates(&candidates, false);
	if (candidate_count <= 0)
		PG_RETURN_TEXT_P(cstring_to_text("worker_memory_reset=no-candidate"));

	before_bytes = MemoryContextMemAllocated(TopMemoryContext, true);
	for (i = 0; i < repeat_count; i++)
		(void) fb_summary_service_build_candidate_safe(&candidates[0]);
	after_bytes = MemoryContextMemAllocated(TopMemoryContext, true);

	/*
	 * The worker path should not retain candidate-sized memory after the
	 * per-build context is deleted. Allow a small fixed slack for normal
	 * backend noise during the helper call.
	 */
	reset_ok = after_bytes <= before_bytes + (256 * 1024);
	fb_summary_free_build_candidates(candidates);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf("worker_memory_reset=%s before=%zu after=%zu",
											  reset_ok ? "true" : "false",
											  before_bytes,
											  after_bytes)));
}

Datum
fb_summary_service_cleanup_debug(PG_FUNCTION_ARGS)
{
	fb_runtime_ensure_initialized();
	fb_summary_service_run_cleanup();
	PG_RETURN_TEXT_P(cstring_to_text("summary_cleanup_ran=true"));
}

Datum
fb_summary_service_schedule_debug(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	FbSummaryServiceScheduleDebugContext *ctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc tupdesc;
		int total_candidates = PG_GETARG_INT32(0);
		int hot_window = PG_GETARG_INT32(1);
		int queue_capacity = PG_GETARG_INT32(2);
		int enqueue_limit;
		int enqueue_count = 0;
		int i;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		ctx = palloc0(sizeof(*ctx));
		ctx->total_candidates = Max(0, total_candidates);
		ctx->hot_window = Max(0, hot_window);
		ctx->queue_capacity = Max(0, queue_capacity);
		if (ctx->total_candidates > 0)
		{
			ctx->enqueued = palloc0(sizeof(bool) * ctx->total_candidates);
			ctx->enqueue_order = palloc0(sizeof(int) * ctx->total_candidates);
			enqueue_limit = Min(ctx->total_candidates, ctx->queue_capacity);

			for (i = ctx->total_candidates - 1;
				 i >= 0 && enqueue_count < enqueue_limit;
				 i--)
			{
				int recent_rank = fb_summary_service_recent_rank_for_index(i,
																		  ctx->total_candidates);
				int queue_kind = fb_summary_service_queue_kind_for_rank(recent_rank,
																		 ctx->hot_window);

				if (queue_kind != FB_SUMMARY_SERVICE_QUEUE_HOT)
					continue;
				ctx->enqueued[i] = true;
				ctx->enqueue_order[i] = ++enqueue_count;
			}
			for (i = 0; i < ctx->total_candidates && enqueue_count < enqueue_limit; i++)
			{
				int recent_rank = fb_summary_service_recent_rank_for_index(i,
																		  ctx->total_candidates);
				int queue_kind = fb_summary_service_queue_kind_for_rank(recent_rank,
																		 ctx->hot_window);

				if (queue_kind != FB_SUMMARY_SERVICE_QUEUE_COLD)
					continue;
				ctx->enqueued[i] = true;
				ctx->enqueue_order[i] = ++enqueue_count;
			}
		}
		ctx->index = 0;
		funcctx->user_fctx = ctx;
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("fb_summary_service_schedule_debug must return a composite type")));
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	ctx = (FbSummaryServiceScheduleDebugContext *) funcctx->user_fctx;

	while (ctx->index < ctx->total_candidates)
	{
		int candidate_index = ctx->index++;
		int recent_rank = fb_summary_service_recent_rank_for_index(candidate_index,
																   ctx->total_candidates);
		int queue_kind = fb_summary_service_queue_kind_for_rank(recent_rank,
																 ctx->hot_window);
		Datum values[6];
		bool nulls[6];
		HeapTuple tuple;

		MemSet(nulls, 0, sizeof(nulls));
		values[0] = Int32GetDatum(candidate_index + 1);
		values[1] = Int32GetDatum(recent_rank);
		values[2] = CStringGetTextDatum(fb_summary_service_queue_kind_name(queue_kind));
		values[3] = BoolGetDatum(ctx->enqueued != NULL && ctx->enqueued[candidate_index]);
		if (ctx->enqueue_order == NULL || ctx->enqueue_order[candidate_index] == 0)
			nulls[4] = true;
		else
			values[4] = Int32GetDatum(ctx->enqueue_order[candidate_index]);
		values[5] = Int32GetDatum(ctx->queue_capacity);
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(funcctx);
}

Datum
fb_summary_cleanup_plan_debug(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	FbSummaryCleanupPlanDebugContext *ctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		ctx = palloc0(sizeof(*ctx));
		ctx->index = 0;
		ctx->active_hashes = NULL;
		ctx->active_count = 0;
		fb_summary_service_collect_protected_hashes(&ctx->active_hashes,
													 &ctx->active_count,
													 false);
		if (fb_summary_service != NULL)
		{
			LWLockAcquire(fb_summary_service_lock, LW_SHARED);
			ctx->snapshot_valid = fb_summary_service->snapshot_valid;
			ctx->snapshot_timeline_id = fb_summary_service->snapshot_timeline_id;
			ctx->snapshot_oldest_segno = fb_summary_service->snapshot_oldest_segno;
			ctx->snapshot_newest_segno = fb_summary_service->snapshot_newest_segno;
			LWLockRelease(fb_summary_service_lock);
		}
		ctx->candidate_count = fb_summary_collect_build_candidates(&ctx->candidates, true);
		funcctx->user_fctx = ctx;
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("fb_summary_cleanup_plan_debug must return a composite type")));
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	ctx = (FbSummaryCleanupPlanDebugContext *) funcctx->user_fctx;

	while (ctx->index < ctx->candidate_count)
	{
		FbSummaryBuildCandidate *candidate = &ctx->candidates[ctx->index++];
		uint64 hash;
		Datum values[5];
		bool nulls[5];
		HeapTuple tuple;
		bool in_snapshot;

		in_snapshot = fb_summary_service_candidate_in_snapshot(ctx->snapshot_valid,
																 ctx->snapshot_timeline_id,
																 ctx->snapshot_oldest_segno,
																 ctx->snapshot_newest_segno,
																 candidate);
		if (!in_snapshot)
			continue;
		hash = fb_summary_candidate_identity_hash(candidate);
		MemSet(nulls, 0, sizeof(nulls));
		values[0] = Int32GetDatum((int32) candidate->timeline_id);
		values[1] = Int64GetDatum((int64) candidate->segno);
		values[2] = BoolGetDatum(fb_summary_candidate_summary_exists(candidate));
		values[3] = BoolGetDatum(in_snapshot);
		values[4] = BoolGetDatum(fb_summary_service_hash_in_list(ctx->active_hashes,
																  ctx->active_count,
																  hash));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	if (ctx->candidates != NULL)
	{
		fb_summary_free_build_candidates(ctx->candidates);
		ctx->candidates = NULL;
	}
	if (ctx->active_hashes != NULL)
	{
		pfree(ctx->active_hashes);
		ctx->active_hashes = NULL;
	}
	SRF_RETURN_DONE(funcctx);
}
