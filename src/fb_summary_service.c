/*
 * fb_summary_service.c
 *    Background summary prebuild launcher and workers.
 */

#include "postgres.h"

#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "fmgr.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
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

typedef enum FbSummaryServiceTaskState
{
	FB_SUMMARY_SERVICE_TASK_EMPTY = 0,
	FB_SUMMARY_SERVICE_TASK_PENDING,
	FB_SUMMARY_SERVICE_TASK_RUNNING
} FbSummaryServiceTaskState;

typedef struct FbSummaryServiceTask
{
	int state;
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
	uint64 scan_count;
	uint64 enqueue_count;
	uint64 build_count;
	uint64 cleanup_count;
	TimestampTz last_scan_at;
	FbSummaryServiceTask tasks[FLEXIBLE_ARRAY_MEMBER];
} FbSummaryServiceShared;

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
static bool fb_summary_service_queue_contains_locked(uint64 file_identity_hash);
static bool fb_summary_service_try_enqueue(const FbSummaryBuildCandidate *candidate);
static bool fb_summary_service_claim_task(FbSummaryBuildCandidate *candidate_out,
											uint64 *identity_hash_out,
											int *slot_index_out);
static void fb_summary_service_finish_task(int slot_index, bool built);
static void fb_summary_service_run_scan(void);
static void fb_summary_service_run_cleanup(void);

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
fb_summary_service_queue_contains_locked(uint64 file_identity_hash)
{
	int i;

	for (i = 0; i < fb_summary_service->queue_capacity; i++)
	{
		FbSummaryServiceTask *task = &fb_summary_service->tasks[i];

		if (task->state == FB_SUMMARY_SERVICE_TASK_EMPTY)
			continue;
		if (task->file_identity_hash == file_identity_hash)
			return true;
	}
	return false;
}

static bool
fb_summary_service_try_enqueue(const FbSummaryBuildCandidate *candidate)
{
	uint64 file_identity_hash;
	int i;
	bool queued = false;

	if (candidate == NULL)
		return false;
	file_identity_hash = fb_summary_candidate_identity_hash(candidate);
	if (file_identity_hash == 0)
		return false;
	if (fb_summary_candidate_summary_exists(candidate))
		return false;

	LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
	if (fb_summary_service_queue_contains_locked(file_identity_hash))
	{
		LWLockRelease(fb_summary_service_lock);
		return false;
	}

	for (i = 0; i < fb_summary_service->queue_capacity; i++)
	{
		FbSummaryServiceTask *task = &fb_summary_service->tasks[i];

		if (task->state != FB_SUMMARY_SERVICE_TASK_EMPTY)
			continue;
		MemSet(task, 0, sizeof(*task));
		task->state = FB_SUMMARY_SERVICE_TASK_PENDING;
		task->file_identity_hash = file_identity_hash;
		task->candidate = *candidate;
		fb_summary_service->enqueue_count++;
		queued = true;
		break;
	}
	LWLockRelease(fb_summary_service_lock);
	return queued;
}

static bool
fb_summary_service_claim_task(FbSummaryBuildCandidate *candidate_out,
							  uint64 *identity_hash_out,
							  int *slot_index_out)
{
	int i;

	LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
	for (i = 0; i < fb_summary_service->queue_capacity; i++)
	{
		FbSummaryServiceTask *task = &fb_summary_service->tasks[i];

		if (task->state != FB_SUMMARY_SERVICE_TASK_PENDING)
			continue;
		task->state = FB_SUMMARY_SERVICE_TASK_RUNNING;
		task->owner_pid = MyProcPid;
		task->attempt_count++;
		task->lease_started_at = GetCurrentTimestamp();
		if (candidate_out != NULL)
			*candidate_out = task->candidate;
		if (identity_hash_out != NULL)
			*identity_hash_out = task->file_identity_hash;
		if (slot_index_out != NULL)
			*slot_index_out = i;
		LWLockRelease(fb_summary_service_lock);
		return true;
	}
	LWLockRelease(fb_summary_service_lock);
	return false;
}

static void
fb_summary_service_finish_task(int slot_index, bool built)
{
	if (slot_index < 0 || slot_index >= fb_summary_service->queue_capacity)
		return;

	LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
	if (built)
		fb_summary_service->build_count++;
	MemSet(&fb_summary_service->tasks[slot_index], 0, sizeof(FbSummaryServiceTask));
	LWLockRelease(fb_summary_service_lock);
}

static void
fb_summary_service_run_scan(void)
{
	FbSummaryBuildCandidate *candidates = NULL;
	int candidate_count;
	int i;

	if (!fb_summary_service_enabled())
		return;

	candidate_count = fb_summary_collect_build_candidates(&candidates, true);
	LWLockAcquire(fb_summary_service_lock, LW_EXCLUSIVE);
	fb_summary_service->scan_count++;
	fb_summary_service->last_scan_at = GetCurrentTimestamp();
	LWLockRelease(fb_summary_service_lock);

	for (i = candidate_count - 1; i >= 0; i--)
	{
		if (!fb_summary_service_try_enqueue(&candidates[i]))
			continue;
	}

	fb_summary_free_build_candidates(candidates);
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
	FbSummaryCleanupEntry *entries = NULL;
	int entry_count = 0;
	int entry_capacity = 0;
	uint64 total_bytes;
	uint64 low_bytes;
	time_t now_sec = time(NULL);
	int i;
	uint64 active_hashes[64];
	int active_count = 0;

	total_bytes = fb_summary_meta_summary_size_bytes(NULL);
	if (total_bytes <= fb_summary_service_meta_limit_bytes())
		return;

	low_bytes = (fb_summary_service_meta_limit_bytes() *
				 (uint64) fb_summary_service_meta_low_watermark_percent()) / 100;

	LWLockAcquire(fb_summary_service_lock, LW_SHARED);
	for (i = 0; i < fb_summary_service->queue_capacity && active_count < lengthof(active_hashes); i++)
	{
		FbSummaryServiceTask *task = &fb_summary_service->tasks[i];

		if (task->state == FB_SUMMARY_SERVICE_TASK_EMPTY)
			continue;
		active_hashes[active_count++] = task->file_identity_hash;
	}
	LWLockRelease(fb_summary_service_lock);

	summary_dir = fb_runtime_meta_summary_dir();
	dir = AllocateDir(summary_dir);
	if (dir == NULL)
	{
		pfree(summary_dir);
		return;
	}

	while ((de = ReadDir(dir, summary_dir)) != NULL)
	{
		char path[MAXPGPATH];
		struct stat st;
		unsigned long long parsed_hash = 0;
		bool protected = false;
		int j;

		if (sscanf(de->d_name, "summary-%llx.meta", &parsed_hash) != 1)
			continue;
		snprintf(path, sizeof(path), "%s/%s", summary_dir, de->d_name);
		if (stat(path, &st) != 0)
			continue;
		if ((now_sec - st.st_mtime) < FB_SUMMARY_SERVICE_RECENT_PROTECT_SECS)
			continue;
		for (j = 0; j < active_count; j++)
		{
			if (active_hashes[j] == (uint64) parsed_hash)
			{
				protected = true;
				break;
			}
		}
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
		entries[entry_count].file_identity_hash = (uint64) parsed_hash;
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

	if (entry_count == 0)
		return;

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
	FbSummaryBuildCandidate candidate;
	uint64 identity_hash = 0;
	int slot_index = -1;
	int worker_index = DatumGetInt32(main_arg);

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
		int rc;

		if (fb_summary_service_claim_task(&candidate, &identity_hash, &slot_index))
		{
			bool built = false;

			if (!fb_summary_candidate_summary_exists(&candidate))
				built = fb_summary_build_candidate(&candidate);
			fb_summary_service_finish_task(slot_index, built);
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
