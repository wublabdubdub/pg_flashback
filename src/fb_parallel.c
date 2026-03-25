#include "postgres.h"

#include "access/heapam.h"
#include "access/relation.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/latch.h"
#include "storage/shm_mq.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "fb_apply.h"
#include "fb_error.h"
#include "fb_guc.h"
#include "fb_parallel.h"
#include "fb_progress.h"

#define FB_PARALLEL_QUEUE_SIZE (1024 * 1024)
#define FB_PARALLEL_ERROR_LEN 256
#define FB_PARALLEL_SQL_LEN 8192

typedef enum FbParallelHelperCommand
{
	FB_PARALLEL_HELPER_CREATE = 1,
	FB_PARALLEL_HELPER_DROP = 2
} FbParallelHelperCommand;

typedef enum FbParallelWorkerStatus
{
	FB_PARALLEL_WORKER_INIT = 0,
	FB_PARALLEL_WORKER_READY = 1,
	FB_PARALLEL_WORKER_APPLY_READY = 2,
	FB_PARALLEL_WORKER_EMITTING = 3,
	FB_PARALLEL_WORKER_DONE = 4,
	FB_PARALLEL_WORKER_FAILED = 5
} FbParallelWorkerStatus;

typedef enum FbParallelMessageType
{
	FB_PARALLEL_MSG_CURRENT = 1,
	FB_PARALLEL_MSG_ADD = 2,
	FB_PARALLEL_MSG_REMOVE = 3,
	FB_PARALLEL_MSG_FINISH = 4
} FbParallelMessageType;

typedef struct FbParallelHelperShared
{
	slock_t mutex;
	uint32 status;
	Oid dboid;
	Oid useroid;
	Oid result_relid;
	char result_name[NAMEDATALEN];
	char error_message[FB_PARALLEL_ERROR_LEN];
	char sql[FB_PARALLEL_SQL_LEN];
} FbParallelHelperShared;

typedef struct FbParallelApplyWorkerShared
{
	slock_t mutex;
	uint32 status;
	Oid dboid;
	Oid useroid;
	Oid result_relid;
	FbRelationInfo info;
	int total_workers;
	uint64 memory_limit_bytes;
	pg_atomic_uint32 start_emit;
	pg_atomic_uint64 total_rows;
	pg_atomic_uint64 emitted_rows;
	char error_message[FB_PARALLEL_ERROR_LEN];
} FbParallelApplyWorkerShared;

typedef struct FbParallelMessageHeader
{
	uint32 type;
	uint32 tuple_len;
} FbParallelMessageHeader;

typedef struct FbParallelApplyWorkerHandle
{
	dsm_segment *seg;
	FbParallelApplyWorkerShared *shared;
	shm_mq_handle *mqh;
	BackgroundWorkerHandle *bgwhandle;
} FbParallelApplyWorkerHandle;

typedef struct FbParallelDispatchState
{
	const FbRelationInfo *info;
	TupleDesc tupdesc;
	FbParallelApplyWorkerHandle *workers;
	int nworkers;
} FbParallelDispatchState;

typedef struct FbParallelTableSinkState
{
	Relation rel;
	TupleTableSlot *slot;
	BulkInsertState bistate;
	CommandId cid;
	FbParallelApplyWorkerShared *shared;
} FbParallelTableSinkState;

PGDLLEXPORT void fb_parallel_result_helper_main(Datum main_arg);
PGDLLEXPORT void fb_parallel_apply_worker_main(Datum main_arg);

static const Size fb_parallel_apply_worker_offset =
	BUFFERALIGN(sizeof(FbParallelApplyWorkerShared));

static void
fb_parallel_set_error_message(slock_t *mutex, uint32 *status,
								 char *error_message, const char *message)
{
	SpinLockAcquire(mutex);
	if (message != NULL)
		strlcpy(error_message, message, FB_PARALLEL_ERROR_LEN);
	else
		error_message[0] = '\0';
	*status = FB_PARALLEL_WORKER_FAILED;
	SpinLockRelease(mutex);
}

static void
fb_parallel_set_status(slock_t *mutex, uint32 *status, uint32 value)
{
	SpinLockAcquire(mutex);
	*status = value;
	SpinLockRelease(mutex);
}

static uint32
fb_parallel_get_status(slock_t *mutex, uint32 *status)
{
	uint32 value;

	SpinLockAcquire(mutex);
	value = *status;
	SpinLockRelease(mutex);
	return value;
}

static void
fb_parallel_worker_capture_error(slock_t *mutex, uint32 *status,
								   char *error_message)
{
	ErrorData  *edata;

	edata = CopyErrorData();
	FlushErrorState();
	fb_parallel_set_error_message(mutex, status, error_message,
								  edata->message);
	FreeErrorData(edata);
}

static Oid
fb_parallel_resolve_result_name(const char *result_name)
{
	RangeVar   *rv;

	rv = makeRangeVar(NULL, pstrdup(result_name), -1);
	return RangeVarGetRelid(rv, AccessShareLock, false);
}

static HeapTuple
fb_parallel_copy_tuple_from_bytes(const char *data, uint32 tuple_len)
{
	HeapTuple tuple;

	tuple = (HeapTuple) palloc0(HEAPTUPLESIZE + tuple_len);
	tuple->t_len = tuple_len;
	ItemPointerSetInvalid(&tuple->t_self);
	tuple->t_tableOid = InvalidOid;
	tuple->t_data = (HeapTupleHeader) ((char *) tuple + HEAPTUPLESIZE);
	memcpy(tuple->t_data, data, tuple_len);
	return tuple;
}

static void
fb_parallel_sink_put_tuple(HeapTuple tuple, TupleDesc tupdesc, void *arg)
{
	FbParallelTableSinkState *state = (FbParallelTableSinkState *) arg;

	(void) tupdesc;

	ExecStoreHeapTuple(tuple, state->slot, false);
	table_tuple_insert(state->rel, state->slot, state->cid, 0, state->bistate);
	ExecClearTuple(state->slot);
	pg_atomic_fetch_add_u64(&state->shared->emitted_rows, 1);
}

static void
fb_parallel_helper_execute(FbParallelHelperShared *shared)
{
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(shared->dboid, shared->useroid, 0);

	PG_TRY();
	{
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed in flashback helper");
		if (SPI_execute(shared->sql, false, 0) != SPI_OK_UTILITY)
			elog(ERROR, "flashback helper SPI utility failed");
		if (SPI_finish() != SPI_OK_FINISH)
			elog(ERROR, "SPI_finish failed in flashback helper");
		CommandCounterIncrement();
		if (shared->status == FB_PARALLEL_HELPER_CREATE)
			shared->result_relid = fb_parallel_resolve_result_name(shared->result_name);
		PopActiveSnapshot();
		CommitTransactionCommand();
		fb_parallel_set_status(&shared->mutex, &shared->status,
							   FB_PARALLEL_WORKER_DONE);
	}
	PG_CATCH();
	{
		if (IsTransactionState())
			AbortCurrentTransaction();
		fb_parallel_worker_capture_error(&shared->mutex,
										 &shared->status,
										 shared->error_message);
	}
	PG_END_TRY();
}

void
fb_parallel_result_helper_main(Datum main_arg)
{
	dsm_segment *seg;
	FbParallelHelperShared *shared;

	pqsignal(SIGTERM, die);

	seg = dsm_attach(DatumGetUInt32(main_arg));
	if (seg == NULL)
		return;

	shared = (FbParallelHelperShared *) dsm_segment_address(seg);
	fb_parallel_helper_execute(shared);
	dsm_detach(seg);
}

static void
fb_parallel_send_message(FbParallelApplyWorkerHandle *worker,
						   FbParallelMessageType type,
						   HeapTuple tuple)
{
	FbParallelMessageHeader hdr;
	shm_mq_iovec iov[2];
	int iovcnt = 1;

	hdr.type = (uint32) type;
	hdr.tuple_len = (tuple == NULL) ? 0 : tuple->t_len;
	iov[0].data = (const char *) &hdr;
	iov[0].len = sizeof(hdr);
	if (tuple != NULL)
	{
		iov[1].data = (const char *) tuple->t_data;
		iov[1].len = tuple->t_len;
		iovcnt = 2;
	}

	if (shm_mq_sendv(worker->mqh, iov, iovcnt, false, true) != SHM_MQ_SUCCESS)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to send flashback parallel work item")));
}

static int
fb_parallel_choose_worker(const FbRelationInfo *info, TupleDesc tupdesc,
						  HeapTuple tuple, int nworkers)
{
	char	   *identity;
	uint32		hash_value;
	int			worker_no;

	if (info->mode == FB_APPLY_KEYED)
		identity = fb_apply_build_key_identity(info, tuple, tupdesc);
	else
		identity = fb_apply_build_row_identity(tuple, tupdesc);

	hash_value = fb_apply_hash_identity(identity);
	worker_no = (int) (hash_value % (uint32) nworkers);
	pfree(identity);
	return worker_no;
}

static void
fb_parallel_dispatch_current_tuple(HeapTuple tuple, TupleDesc tupdesc, void *arg)
{
	FbParallelDispatchState *state = (FbParallelDispatchState *) arg;
	int worker_no;

	worker_no = fb_parallel_choose_worker(state->info,
										  state->tupdesc,
										  tuple,
										  state->nworkers);
	fb_parallel_send_message(&state->workers[worker_no],
							 FB_PARALLEL_MSG_CURRENT,
							 tuple);
	heap_freetuple(tuple);
}

static void
fb_parallel_dispatch_reverse_ops(const FbRelationInfo *info,
								 TupleDesc tupdesc,
								 const FbReverseOpStream *stream,
								 FbParallelApplyWorkerHandle *workers,
								 int nworkers,
								 const char *progress_detail)
{
	uint32 i;

	if (stream->count == 0)
	{
		fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY, 80, progress_detail);
		return;
	}

	for (i = 0; i < stream->count; i++)
	{
		const FbReverseOp *op = &stream->ops[i];

		switch (op->type)
		{
			case FB_REVERSE_REMOVE:
				if (op->new_row.tuple != NULL)
					fb_parallel_send_message(&workers[fb_parallel_choose_worker(info,
																			 tupdesc,
																			 op->new_row.tuple,
																			 nworkers)],
											 FB_PARALLEL_MSG_REMOVE,
											 op->new_row.tuple);
				break;
			case FB_REVERSE_ADD:
				if (op->old_row.tuple != NULL)
					fb_parallel_send_message(&workers[fb_parallel_choose_worker(info,
																			 tupdesc,
																			 op->old_row.tuple,
																			 nworkers)],
											 FB_PARALLEL_MSG_ADD,
											 op->old_row.tuple);
				break;
			case FB_REVERSE_REPLACE:
				if (op->new_row.tuple != NULL)
					fb_parallel_send_message(&workers[fb_parallel_choose_worker(info,
																			 tupdesc,
																			 op->new_row.tuple,
																			 nworkers)],
											 FB_PARALLEL_MSG_REMOVE,
											 op->new_row.tuple);
				if (op->old_row.tuple != NULL)
					fb_parallel_send_message(&workers[fb_parallel_choose_worker(info,
																			 tupdesc,
																			 op->old_row.tuple,
																			 nworkers)],
											 FB_PARALLEL_MSG_ADD,
											 op->old_row.tuple);
				break;
		}

		fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY,
								   fb_progress_map_subrange(40, 40,
															(uint64) i + 1,
															stream->count),
								   progress_detail);
	}
}

static void
fb_parallel_start_worker(BackgroundWorker *worker, BackgroundWorkerHandle **handle)
{
	pid_t pid = 0;

	if (!RegisterDynamicBackgroundWorker(worker, handle))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("failed to register flashback parallel background worker")));

	if (WaitForBackgroundWorkerStartup(*handle, &pid) != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("flashback parallel background worker failed to start")));
}

static Oid
fb_parallel_create_result_relation(const char *result_name, const char *create_sql)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle = NULL;
	dsm_segment *seg;
	FbParallelHelperShared *shared;
	uint32 status;
	Oid result_relid;

	if (strlen(create_sql) >= FB_PARALLEL_SQL_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("flashback parallel create SQL is too large")));

	seg = dsm_create(BUFFERALIGN(sizeof(FbParallelHelperShared)), 0);
	shared = (FbParallelHelperShared *) dsm_segment_address(seg);
	MemSet(shared, 0, sizeof(*shared));
	SpinLockInit(&shared->mutex);
	shared->status = FB_PARALLEL_HELPER_CREATE;
	shared->dboid = MyDatabaseId;
	shared->useroid = GetUserId();
	strlcpy(shared->result_name, result_name, sizeof(shared->result_name));
	strlcpy(shared->sql, create_sql, sizeof(shared->sql));

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_flashback create %s", result_name);
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_flashback helper");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "pg_flashback");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "fb_parallel_result_helper_main");
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
	worker.bgw_notify_pid = MyProcPid;

	fb_parallel_start_worker(&worker, &handle);
	if (WaitForBackgroundWorkerShutdown(handle) != BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("flashback result create helper did not stop cleanly")));

	status = fb_parallel_get_status(&shared->mutex, &shared->status);
	if (status != FB_PARALLEL_WORKER_DONE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("flashback parallel result create failed: %s",
						shared->error_message[0] ? shared->error_message : "unknown error")));

	result_relid = shared->result_relid;
	dsm_detach(seg);
	return result_relid;
}

bool
fb_parallel_cleanup_result(const char *result_name)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle = NULL;
	dsm_segment *seg;
	FbParallelHelperShared *shared;
	char *drop_sql;
	uint32 status;

	drop_sql = psprintf("DROP TABLE IF EXISTS %s", quote_identifier(result_name));
	if (strlen(drop_sql) >= FB_PARALLEL_SQL_LEN)
		return false;

	seg = dsm_create(BUFFERALIGN(sizeof(FbParallelHelperShared)), 0);
	shared = (FbParallelHelperShared *) dsm_segment_address(seg);
	MemSet(shared, 0, sizeof(*shared));
	SpinLockInit(&shared->mutex);
	shared->status = FB_PARALLEL_HELPER_DROP;
	shared->dboid = MyDatabaseId;
	shared->useroid = GetUserId();
	strlcpy(shared->result_name, result_name, sizeof(shared->result_name));
	strlcpy(shared->sql, drop_sql, sizeof(shared->sql));

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_flashback drop %s", result_name);
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_flashback helper");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "pg_flashback");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "fb_parallel_result_helper_main");
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
	worker.bgw_notify_pid = MyProcPid;

	fb_parallel_start_worker(&worker, &handle);
	if (WaitForBackgroundWorkerShutdown(handle) != BGWH_STOPPED)
		return false;

	status = fb_parallel_get_status(&shared->mutex, &shared->status);
	dsm_detach(seg);
	return status == FB_PARALLEL_WORKER_DONE;
}

static void
fb_parallel_setup_apply_worker(FbParallelApplyWorkerHandle *worker,
								 const FbRelationInfo *info,
								 Oid result_relid,
								 int total_workers)
{
	Size segsize;
	shm_mq *mq;

	segsize = fb_parallel_apply_worker_offset + FB_PARALLEL_QUEUE_SIZE;
	worker->seg = dsm_create(segsize, 0);
	worker->shared = (FbParallelApplyWorkerShared *) dsm_segment_address(worker->seg);
	MemSet(worker->shared, 0, sizeof(*worker->shared));
	SpinLockInit(&worker->shared->mutex);
	worker->shared->status = FB_PARALLEL_WORKER_INIT;
	worker->shared->dboid = MyDatabaseId;
	worker->shared->useroid = GetUserId();
	worker->shared->result_relid = result_relid;
	worker->shared->info = *info;
	worker->shared->total_workers = total_workers;
	worker->shared->memory_limit_bytes = fb_get_memory_limit_bytes();
	pg_atomic_init_u32(&worker->shared->start_emit, 0);
	pg_atomic_init_u64(&worker->shared->total_rows, 0);
	pg_atomic_init_u64(&worker->shared->emitted_rows, 0);

	mq = shm_mq_create((char *) worker->shared + fb_parallel_apply_worker_offset,
					   FB_PARALLEL_QUEUE_SIZE);
	shm_mq_set_sender(mq, MyProc);
	worker->mqh = shm_mq_attach(mq, worker->seg, NULL);
}

static void
fb_parallel_launch_apply_worker(FbParallelApplyWorkerHandle *worker,
								  const char *result_name)
{
	BackgroundWorker bgworker;

	MemSet(&bgworker, 0, sizeof(bgworker));
	snprintf(bgworker.bgw_name, BGW_MAXLEN, "pg_flashback apply %s", result_name);
	snprintf(bgworker.bgw_type, BGW_MAXLEN, "pg_flashback apply");
	bgworker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgworker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgworker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(bgworker.bgw_library_name, MAXPGPATH, "pg_flashback");
	snprintf(bgworker.bgw_function_name, BGW_MAXLEN, "fb_parallel_apply_worker_main");
	bgworker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(worker->seg));
	bgworker.bgw_notify_pid = MyProcPid;

	fb_parallel_start_worker(&bgworker, &worker->bgwhandle);
	shm_mq_set_handle(worker->mqh, worker->bgwhandle);
	if (shm_mq_wait_for_attach(worker->mqh) != SHM_MQ_SUCCESS)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("flashback apply worker failed to attach queue")));
}

static void
fb_parallel_apply_worker_execute(FbParallelApplyWorkerShared *shared,
								   shm_mq_handle *mqh)
{
	Relation result_rel;
	TupleDesc tupdesc;
	TupleTableSlot *write_slot;
	FbParallelTableSinkState sink_state;
	FbResultSink sink;
	Size nbytes;
	void *data;
	shm_mq_result mq_result;
	FbKeyedState *keyed_state = NULL;
	FbBagState *bag_state = NULL;
	uint64 worker_limit;

	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(shared->dboid, shared->useroid, 0);

	worker_limit = shared->memory_limit_bytes;
	if (shared->total_workers > 0)
		worker_limit = Max((uint64) 1024, worker_limit / (uint64) shared->total_workers);

	PG_TRY();
	{
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		result_rel = relation_open(shared->result_relid, RowExclusiveLock);
		tupdesc = CreateTupleDescCopy(RelationGetDescr(result_rel));
		write_slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsHeapTuple);
		sink_state.rel = result_rel;
		sink_state.slot = write_slot;
		sink_state.bistate = GetBulkInsertState();
		sink_state.cid = GetCurrentCommandId(true);
		sink_state.shared = shared;
		sink.put_tuple = fb_parallel_sink_put_tuple;
		sink.arg = &sink_state;

		if (shared->info.mode == FB_APPLY_KEYED)
			keyed_state = fb_keyed_state_create(&shared->info, tupdesc, 0, worker_limit);
		else
			bag_state = fb_bag_state_create(tupdesc, 0, worker_limit);

		fb_parallel_set_status(&shared->mutex, &shared->status,
							   FB_PARALLEL_WORKER_READY);

		for (;;)
		{
			FbParallelMessageHeader *hdr;
			HeapTuple tuple = NULL;

			mq_result = shm_mq_receive(mqh, &nbytes, &data, false);
			if (mq_result != SHM_MQ_SUCCESS)
				elog(ERROR, "flashback parallel worker queue receive failed");

			hdr = (FbParallelMessageHeader *) data;
			if (hdr->tuple_len > 0)
				tuple = fb_parallel_copy_tuple_from_bytes((char *) data + sizeof(*hdr),
														 hdr->tuple_len);

			if (hdr->type == FB_PARALLEL_MSG_FINISH)
				break;

			if (shared->info.mode == FB_APPLY_KEYED)
			{
				if (hdr->type == FB_PARALLEL_MSG_CURRENT)
					fb_keyed_state_add_current_tuple(keyed_state, tuple);
				else if (hdr->type == FB_PARALLEL_MSG_ADD)
					fb_keyed_state_apply_add_tuple(keyed_state, tuple);
				else if (hdr->type == FB_PARALLEL_MSG_REMOVE)
					fb_keyed_state_apply_remove_tuple(keyed_state, tuple);
			}
			else
			{
				if (hdr->type == FB_PARALLEL_MSG_CURRENT)
					fb_bag_state_add_current_tuple(bag_state, tuple);
				else if (hdr->type == FB_PARALLEL_MSG_ADD)
					fb_bag_state_apply_add_tuple(bag_state, tuple);
				else if (hdr->type == FB_PARALLEL_MSG_REMOVE)
					fb_bag_state_apply_remove_tuple(bag_state, tuple);
			}
		}

		if (shared->info.mode == FB_APPLY_KEYED)
			pg_atomic_write_u64(&shared->total_rows,
								fb_keyed_state_count_rows(keyed_state));
		else
			pg_atomic_write_u64(&shared->total_rows,
								fb_bag_state_count_rows(bag_state));

		fb_parallel_set_status(&shared->mutex, &shared->status,
							   FB_PARALLEL_WORKER_APPLY_READY);

		while (pg_atomic_read_u32(&shared->start_emit) == 0)
			pg_usleep(10000L);

		fb_parallel_set_status(&shared->mutex, &shared->status,
							   FB_PARALLEL_WORKER_EMITTING);
		if (shared->info.mode == FB_APPLY_KEYED)
			(void) fb_keyed_state_emit_rows(keyed_state, tupdesc, &sink);
		else
			(void) fb_bag_state_emit_rows(bag_state, tupdesc, &sink);

		table_finish_bulk_insert(result_rel, 0);
		FreeBulkInsertState(sink_state.bistate);
		ExecDropSingleTupleTableSlot(write_slot);
		relation_close(result_rel, RowExclusiveLock);
		PopActiveSnapshot();
		CommitTransactionCommand();
		fb_parallel_set_status(&shared->mutex, &shared->status,
							   FB_PARALLEL_WORKER_DONE);
	}
	PG_CATCH();
	{
		if (IsTransactionState())
			AbortCurrentTransaction();
		fb_parallel_worker_capture_error(&shared->mutex,
										 &shared->status,
										 shared->error_message);
	}
	PG_END_TRY();
}

void
fb_parallel_apply_worker_main(Datum main_arg)
{
	dsm_segment *seg;
	FbParallelApplyWorkerShared *shared;
	shm_mq *mq;
	shm_mq_handle *mqh;

	pqsignal(SIGTERM, die);

	seg = dsm_attach(DatumGetUInt32(main_arg));
	if (seg == NULL)
		return;

	shared = (FbParallelApplyWorkerShared *) dsm_segment_address(seg);
	mq = (shm_mq *) ((char *) shared + fb_parallel_apply_worker_offset);
	shm_mq_set_receiver(mq, MyProc);
	mqh = shm_mq_attach(mq, seg, NULL);

	fb_parallel_apply_worker_execute(shared, mqh);
	dsm_detach(seg);
}

static uint64
fb_parallel_sum_total_rows(FbParallelApplyWorkerHandle *workers, int nworkers)
{
	uint64 total = 0;
	int i;

	for (i = 0; i < nworkers; i++)
		total += pg_atomic_read_u64(&workers[i].shared->total_rows);
	return total;
}

static uint64
fb_parallel_sum_emitted_rows(FbParallelApplyWorkerHandle *workers, int nworkers)
{
	uint64 emitted = 0;
	int i;

	for (i = 0; i < nworkers; i++)
		emitted += pg_atomic_read_u64(&workers[i].shared->emitted_rows);
	return emitted;
}

static int
fb_parallel_count_workers_in_status(FbParallelApplyWorkerHandle *workers,
									 int nworkers, FbParallelWorkerStatus status)
{
	int i;
	int count = 0;

	for (i = 0; i < nworkers; i++)
	{
		uint32 current = fb_parallel_get_status(&workers[i].shared->mutex,
												 &workers[i].shared->status);
		if (current == status)
			count++;
	}
	return count;
}

static void
fb_parallel_check_workers_failed(FbParallelApplyWorkerHandle *workers, int nworkers)
{
	int i;

	for (i = 0; i < nworkers; i++)
	{
		uint32 status = fb_parallel_get_status(&workers[i].shared->mutex,
												 &workers[i].shared->status);
		if (status == FB_PARALLEL_WORKER_FAILED)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("flashback parallel apply worker failed: %s",
							workers[i].shared->error_message[0] ?
							workers[i].shared->error_message :
							"unknown error")));
	}
}

uint64
fb_parallel_apply_reverse_ops(const char *result_name,
								const char *create_sql,
								const FbRelationInfo *info,
								TupleDesc tupdesc,
								const FbReverseOpStream *stream)
{
	FbParallelApplyWorkerHandle *workers;
	FbParallelDispatchState dispatch;
	char detail[64];
	int nworkers = fb_parallel_apply_workers();
	int i;
	Oid result_relid;
	uint64 total_rows;
	bool created_result = false;

	workers = NULL;

	if (nworkers <= 0)
		elog(ERROR, "parallel flashback apply requested with no workers");

	PG_TRY();
	{
		result_relid = fb_parallel_create_result_relation(result_name, create_sql);
		created_result = true;
		workers = palloc0(sizeof(*workers) * nworkers);
		for (i = 0; i < nworkers; i++)
		{
			fb_parallel_setup_apply_worker(&workers[i], info, result_relid, nworkers);
			fb_parallel_launch_apply_worker(&workers[i], result_name);
		}

		snprintf(detail, sizeof(detail), "parallel workers=%d", nworkers);
		fb_progress_enter_stage(FB_PROGRESS_STAGE_APPLY, detail);

		dispatch.info = info;
		dispatch.tupdesc = tupdesc;
		dispatch.workers = workers;
		dispatch.nworkers = nworkers;
		fb_apply_scan_current_relation(info->relid,
									   fb_parallel_dispatch_current_tuple,
									   &dispatch,
									   detail);
		fb_parallel_dispatch_reverse_ops(info, tupdesc, stream, workers, nworkers, detail);

		for (i = 0; i < nworkers; i++)
			fb_parallel_send_message(&workers[i], FB_PARALLEL_MSG_FINISH, NULL);

		for (;;)
		{
			int ready = fb_parallel_count_workers_in_status(workers,
															 nworkers,
															 FB_PARALLEL_WORKER_APPLY_READY);

			fb_parallel_check_workers_failed(workers, nworkers);
			fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY,
									   fb_progress_map_subrange(80, 20,
																(uint64) ready,
																(uint64) nworkers),
									   detail);
			if (ready >= nworkers)
				break;
			pg_usleep(10000L);
		}

		total_rows = fb_parallel_sum_total_rows(workers, nworkers);
		fb_progress_enter_stage(FB_PROGRESS_STAGE_MATERIALIZE, NULL);
		if (total_rows > 0)
			fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 20, NULL);
		for (i = 0; i < nworkers; i++)
			pg_atomic_write_u32(&workers[i].shared->start_emit, 1);

		if (total_rows == 0)
			fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);

		for (;;)
		{
			int done = fb_parallel_count_workers_in_status(workers,
														   nworkers,
														   FB_PARALLEL_WORKER_DONE);
			uint64 emitted = fb_parallel_sum_emitted_rows(workers, nworkers);

			fb_parallel_check_workers_failed(workers, nworkers);
			if (total_rows > 0)
				fb_progress_update_fraction(FB_PROGRESS_STAGE_MATERIALIZE,
											emitted,
											total_rows,
											NULL);
			if (done >= nworkers)
				break;
			pg_usleep(10000L);
		}

		for (i = 0; i < nworkers; i++)
		{
			if (WaitForBackgroundWorkerShutdown(workers[i].bgwhandle) != BGWH_STOPPED)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("flashback apply worker did not stop cleanly")));
			shm_mq_detach(workers[i].mqh);
			dsm_detach(workers[i].seg);
		}
	}
	PG_CATCH();
	{
		if (workers != NULL)
		{
			for (i = 0; i < nworkers; i++)
			{
				if (workers[i].bgwhandle != NULL)
					TerminateBackgroundWorker(workers[i].bgwhandle);
				if (workers[i].mqh != NULL)
					shm_mq_detach(workers[i].mqh);
				if (workers[i].seg != NULL)
					dsm_detach(workers[i].seg);
			}
		}
		if (created_result)
			(void) fb_parallel_cleanup_result(result_name);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return total_rows;
}
