/*
 * fb_replay.c
 *    Page replay and forward-op extraction.
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/relation.h"
#include "access/heapam_xlog.h"
#include "nodes/bitmapset.h"
#include "storage/bufpage.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "fb_compat.h"
#include "fb_memory.h"
#include "fb_progress.h"
#include "fb_replay.h"
#include "fb_summary.h"
#include "fb_toast.h"

PG_FUNCTION_INFO_V1(fb_replay_apply_image_contract_debug);
PG_FUNCTION_INFO_V1(fb_replay_nonapply_image_missing_contract_debug);
PG_FUNCTION_INFO_V1(fb_replay_heap_update_same_block_init_contract_debug);
PG_FUNCTION_INFO_V1(fb_replay_heap_update_block_id_contract_debug);
PG_FUNCTION_INFO_V1(fb_replay_prune_image_short_circuit_debug);
PG_FUNCTION_INFO_V1(fb_replay_prune_image_preserve_next_insert_debug);
PG_FUNCTION_INFO_V1(fb_replay_prune_image_preserve_next_multi_insert_debug);
PG_FUNCTION_INFO_V1(fb_replay_prune_image_preserve_dead_old_tuple_debug);
PG_FUNCTION_INFO_V1(fb_replay_prune_image_reject_used_insert_slot_debug);
PG_FUNCTION_INFO_V1(fb_replay_prune_image_reject_future_warm_state_debug);
PG_FUNCTION_INFO_V1(fb_replay_prune_compose_future_constraints_debug);
PG_FUNCTION_INFO_V1(fb_replay_prune_lookahead_snapshot_isolation_debug);

/*
 * FbReplayBlockKey
 *    Private replay structure.
 */

typedef struct FbReplayBlockKey
{
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blkno;
} FbReplayBlockKey;

/*
 * FbReplayBlockState
 *    Tracks replay block state.
 */

typedef struct FbReplayBlockState
{
	FbReplayBlockKey key;
	bool initialized;
	char page[BLCKSZ];
	XLogRecPtr page_lsn;
} FbReplayBlockState;

/*
 * FbReplayStore
 *    Private replay structure.
 */

typedef struct FbReplayStore
{
	HTAB *blocks;
} FbReplayStore;

typedef struct FbReplayRawWarmVisitorState
{
	const FbRelationInfo *info;
	FbReplayStore *store;
	TupleDesc toast_tupdesc;
	FbToastStore *toast_store;
	HTAB *apply_blocks;
	struct FbReplayControl *control;
	FbReplayResult *result;
} FbReplayRawWarmVisitorState;

typedef struct FbReplayRawAnchorVisitorState
{
	const FbRelationInfo *info;
	HTAB *missing_blocks;
	uint32 unresolved;
} FbReplayRawAnchorVisitorState;

struct FbReplayDiscoverState
{
	HTAB *backtrack_blocks;
	TupleDesc toast_tupdesc;
	uint32 precomputed_missing_blocks;
	uint32 discover_rounds;
	bool discover_skipped;
	uint32 summary_anchor_hits;
	uint32 summary_anchor_fallback;
	uint32 summary_anchor_segments_read;
};

struct FbReplayWarmState
{
	FbReplayStore store;
	FbToastStore *toast_store;
	TupleDesc toast_tupdesc;
	uint64 warm_tracked_bytes;
};

typedef enum FbReplayPhase
{
	FB_REPLAY_PHASE_DISCOVER = 1,
	FB_REPLAY_PHASE_WARM,
	FB_REPLAY_PHASE_FINAL
} FbReplayPhase;

/*
 * FbReplayMissingBlock
 *    Private replay structure.
 */

typedef struct FbReplayMissingBlock
{
	FbReplayBlockKey key;
	uint32 first_record_index;
	XLogRecPtr first_record_lsn;
	XLogRecPtr anchor_lsn;
	bool anchor_found;
} FbReplayMissingBlock;

typedef struct FbReplayFutureBlockRecord
{
	bool valid;
	bool materializes_page;
	Bitmapset *old_tuple_offsets;
	Bitmapset *new_tuple_slot_offsets;
} FbReplayFutureBlockRecord;

typedef struct FbReplayFutureBlockEntry
{
	FbReplayBlockKey key;
	FbReplayFutureBlockRecord future;
} FbReplayFutureBlockEntry;

typedef struct FbReplayPruneLookaheadEntry
{
	uint32 record_index;
	FbReplayFutureBlockRecord future;
} FbReplayPruneLookaheadEntry;

/*
 * FbReplayControl
 *    Private replay structure.
 */

typedef struct FbReplayControl
{
	FbReplayPhase phase;
	uint32 record_index;
	uint32 round_no;
	HTAB *missing_blocks;
	const FbWalRecordIndex *index;
	HTAB *prune_lookahead;
} FbReplayControl;

static void fb_replay_debug_log_toast17079_page(const char *label,
												 XLogRecPtr lsn,
												 Page page);
static void fb_replay_debug_log_toast26273_page(const char *label,
												 XLogRecPtr lsn,
												 Page page);
static void fb_replay_debug_log_documents38724_page(const char *label,
													XLogRecPtr lsn,
													Page page);


static Size
fb_replay_debug_build_heap_tuple(char *buf,
								 BlockNumber blkno,
								 OffsetNumber offnum)
{
	HeapTupleHeader htup = (HeapTupleHeader) buf;
	const char payload[] = "x";
	Size payload_len = sizeof(payload);
	Size tuple_len = SizeofHeapTupleHeader + payload_len;

	MemSet(buf, 0, tuple_len);
	htup->t_infomask2 = 1;
	htup->t_infomask = HEAP_HASVARWIDTH;
	htup->t_hoff = SizeofHeapTupleHeader;
	HeapTupleHeaderSetXmin(htup, FirstNormalTransactionId);
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	ItemPointerSet(&htup->t_ctid, blkno, offnum);
	memcpy(buf + SizeofHeapTupleHeader, payload, payload_len);

	return tuple_len;
}

static void
fb_replay_debug_seed_page(Page page, BlockNumber blkno, OffsetNumber maxoff)
{
	OffsetNumber offnum;

	PageInit(page, BLCKSZ, 0);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
	{
		char tuple_buf[SizeofHeapTupleHeader + 8];
		Size tuple_len;

		tuple_len = fb_replay_debug_build_heap_tuple(tuple_buf, blkno, offnum);
		if (PageAddItem(page, (Item) tuple_buf, tuple_len,
						offnum, true, true) == InvalidOffsetNumber)
			elog(ERROR, "failed to seed debug replay page");
	}
}

static void
fb_replay_future_block_record_set_materializes(FbReplayFutureBlockRecord *future)
{
	if (future == NULL)
		return;

	future->valid = true;
	future->materializes_page = true;
	future->old_tuple_offsets = NULL;
	future->new_tuple_slot_offsets = NULL;
}

static void
fb_replay_future_block_record_add_old(FbReplayFutureBlockRecord *future,
									  OffsetNumber offnum)
{
	if (future == NULL || offnum < FirstOffsetNumber)
		return;

	future->valid = true;
	future->materializes_page = false;
	future->old_tuple_offsets =
		bms_add_member(future->old_tuple_offsets, (int) offnum);
}

static void
fb_replay_future_block_record_add_new(FbReplayFutureBlockRecord *future,
									  OffsetNumber offnum)
{
	if (future == NULL || offnum < FirstOffsetNumber)
		return;

	future->valid = true;
	future->materializes_page = false;
	future->new_tuple_slot_offsets =
		bms_add_member(future->new_tuple_slot_offsets, (int) offnum);
}

static void
fb_replay_future_block_record_remove_old(FbReplayFutureBlockRecord *future,
										 OffsetNumber offnum)
{
	if (future == NULL || offnum < FirstOffsetNumber)
		return;

	future->old_tuple_offsets =
		bms_del_member(future->old_tuple_offsets, (int) offnum);
}

static void
fb_replay_future_block_record_remove_new(FbReplayFutureBlockRecord *future,
										  OffsetNumber offnum)
{
	if (future == NULL || offnum < FirstOffsetNumber)
		return;

	future->new_tuple_slot_offsets =
		bms_del_member(future->new_tuple_slot_offsets, (int) offnum);
}

static int
fb_replay_future_block_record_old_count(const FbReplayFutureBlockRecord *future)
{
	if (future == NULL || future->materializes_page)
		return 0;

	return bms_num_members(future->old_tuple_offsets);
}

static int
fb_replay_future_block_record_new_count(const FbReplayFutureBlockRecord *future)
{
	if (future == NULL || future->materializes_page)
		return 0;

	return bms_num_members(future->new_tuple_slot_offsets);
}

static OffsetNumber
fb_replay_future_block_record_first_old(const FbReplayFutureBlockRecord *future)
{
	int member;

	if (future == NULL || future->materializes_page)
		return InvalidOffsetNumber;

	member = bms_next_member(future->old_tuple_offsets, -1);
	return (member >= 0) ? (OffsetNumber) member : InvalidOffsetNumber;
}

static OffsetNumber
fb_replay_future_block_record_first_new(const FbReplayFutureBlockRecord *future)
{
	int member;

	if (future == NULL || future->materializes_page)
		return InvalidOffsetNumber;

	member = bms_next_member(future->new_tuple_slot_offsets, -1);
	return (member >= 0) ? (OffsetNumber) member : InvalidOffsetNumber;
}

static void
fb_replay_future_block_record_clone(FbReplayFutureBlockRecord *dst,
									 const FbReplayFutureBlockRecord *src)
{
	if (dst == NULL)
		return;

	MemSet(dst, 0, sizeof(*dst));
	if (src == NULL)
		return;

	dst->valid = src->valid;
	dst->materializes_page = src->materializes_page;
	if (!src->materializes_page)
	{
		dst->old_tuple_offsets = bms_copy(src->old_tuple_offsets);
		dst->new_tuple_slot_offsets = bms_copy(src->new_tuple_slot_offsets);
	}
}

typedef struct FbPendingReverseOpEntry
{
	FbReverseOp op;
	struct FbPendingReverseOpEntry *next;
} FbPendingReverseOpEntry;

typedef struct FbPendingReverseOpQueue
{
	FbPendingReverseOpEntry *head;
	FbPendingReverseOpEntry *tail;
} FbPendingReverseOpQueue;

static FbReplayMissingBlock *fb_replay_find_missing_block(HTAB *missing_blocks,
														  const FbRecordBlockRef *block_ref);
static void fb_replay_seed_missing_blocks_from_index(HTAB *missing_blocks,
													  const FbWalRecordIndex *index);
static XLogRecPtr fb_replay_missing_blocks_end_lsn(HTAB *missing_blocks);
static bool fb_replay_should_apply_raw_backtrack(const FbRecordRef *record,
												 HTAB *missing_blocks);
static void fb_replay_apply_record(const FbRelationInfo *info,
								   const FbRecordRef *record,
								   FbReplayStore *store,
								   TupleDesc tupdesc,
								   TupleDesc toast_tupdesc,
								   FbToastStore *toast_store,
								   FbReplayControl *control,
								   FbReplayResult *result,
								   FbReverseOpSource *source,
								   FbPendingReverseOpQueue *pending_ops);
static bool fb_replay_raw_warm_visitor(XLogReaderState *reader, void *arg);
static bool fb_replay_raw_anchor_visitor(XLogReaderState *reader, void *arg);
static void fb_replay_init_result(const FbWalRecordIndex *index,
								  FbReplayResult *result);
static bool fb_replay_record_can_anchor_block(const FbRecordRef *record,
											   int block_index);
static FbRecordBlockRef *fb_replay_find_block_ref_by_wal_id(FbRecordRef *record,
															 uint8 wal_block_id);

/*
 * fb_replay_progress_stage
 *    Replay helper.
 */

static FbProgressStage
fb_replay_progress_stage(FbReplayPhase phase)
{
	switch (phase)
	{
		case FB_REPLAY_PHASE_DISCOVER:
			return FB_PROGRESS_STAGE_REPLAY_DISCOVER;
		case FB_REPLAY_PHASE_WARM:
			return FB_PROGRESS_STAGE_REPLAY_WARM;
		case FB_REPLAY_PHASE_FINAL:
			return FB_PROGRESS_STAGE_REPLAY_FINAL;
	}

	elog(ERROR, "invalid fb replay phase: %d", (int) phase);
	return FB_PROGRESS_STAGE_REPLAY_FINAL;
}

/*
 * fb_replay_progress_detail
 *    Replay helper.
 */

static char *
fb_replay_progress_detail(FbReplayControl *control)
{
	if (control == NULL || control->phase != FB_REPLAY_PHASE_DISCOVER)
		return NULL;

	return psprintf("round=%u", control->round_no);
}

/*
 * fb_replay_progress_enter
 *    Replay helper.
 */

static void
fb_replay_progress_enter(FbReplayControl *control)
{
	char *detail;

	if (control == NULL)
		return;

	detail = fb_replay_progress_detail(control);
	fb_progress_enter_stage(fb_replay_progress_stage(control->phase), detail);
	if (detail != NULL)
		pfree(detail);
}

/*
 * fb_replay_progress_update
 *    Replay helper.
 */

static void
fb_replay_progress_update(FbReplayControl *control, uint32 current, uint32 total)
{
	char *detail;

	if (control == NULL)
		return;

	detail = fb_replay_progress_detail(control);
	fb_progress_update_fraction(fb_replay_progress_stage(control->phase),
								 current,
								 total,
								 detail);
	if (detail != NULL)
		pfree(detail);
}

/*
 * fb_replay_missing_fpi_error
 *    Replay helper.
 */

static void fb_replay_missing_fpi_error(const FbRecordRef *record,
										const FbRecordBlockRef *block_ref,
										bool allow_init);

/*
 * fb_replay_charge_bytes
 *    Replay helper.
 */

static void
fb_replay_charge_bytes(FbReplayResult *result, Size bytes, const char *what)
{
	if (result == NULL)
		return;

	fb_memory_charge_bytes(&result->tracked_bytes,
						   result->memory_limit_bytes,
						   bytes,
						   what);
}

/*
 * fb_replay_create_block_hash
 *    Replay helper.
 */

static HTAB *
fb_replay_create_block_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(FbReplayBlockKey);
	ctl.entrysize = sizeof(FbReplayBlockState);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create("fb replay blocks", 128, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static TupleDesc
fb_replay_open_toast_tupdesc(const FbRelationInfo *info)
{
	Relation toast_rel;
	TupleDesc toast_tupdesc = NULL;

	if (info == NULL || !info->has_toast_locator || !OidIsValid(info->toast_relid))
		return NULL;

	toast_rel = relation_open(info->toast_relid, AccessShareLock);
	toast_tupdesc = CreateTupleDescCopy(toast_rel->rd_att);
	relation_close(toast_rel, AccessShareLock);
	return toast_tupdesc;
}

/*
 * fb_replay_create_missing_block_hash
 *    Replay helper.
 */

static HTAB *
fb_replay_create_missing_block_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(FbReplayBlockKey);
	ctl.entrysize = sizeof(FbReplayMissingBlock);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create("fb replay missing blocks", 64, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
fb_replay_seed_missing_blocks_from_index(HTAB *missing_blocks,
										 const FbWalRecordIndex *index)
{
	HASH_SEQ_STATUS seq;
	FbWalPrecomputedMissingBlock *entry;

	if (missing_blocks == NULL || index == NULL ||
		index->precomputed_missing_blocks == NULL)
		return;

	hash_seq_init(&seq, index->precomputed_missing_blocks);
	while ((entry = (FbWalPrecomputedMissingBlock *) hash_seq_search(&seq)) != NULL)
	{
		FbReplayMissingBlock *dest;
		bool found = false;

		dest = (FbReplayMissingBlock *) hash_search(missing_blocks,
													 &entry->key,
													 HASH_ENTER,
													 &found);
		if (!found)
		{
			MemSet(dest, 0, sizeof(*dest));
			dest->key.locator = entry->key.locator;
			dest->key.forknum = entry->key.forknum;
			dest->key.blkno = entry->key.blkno;
		}
		dest->first_record_index = entry->first_record_index;
		dest->first_record_lsn = entry->first_record_lsn;
		dest->anchor_lsn = InvalidXLogRecPtr;
		dest->anchor_found = false;
	}
}

/*
 * fb_fix_infomask_from_infobits
 *    Replay helper.
 */

static void
fb_fix_infomask_from_infobits(uint8 infobits, uint16 *infomask, uint16 *infomask2)
{
	*infomask &= ~(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY |
				   HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_EXCL_LOCK);
	*infomask2 &= ~HEAP_KEYS_UPDATED;

	if (infobits & XLHL_XMAX_IS_MULTI)
		*infomask |= HEAP_XMAX_IS_MULTI;
	if (infobits & XLHL_XMAX_LOCK_ONLY)
		*infomask |= HEAP_XMAX_LOCK_ONLY;
	if (infobits & XLHL_XMAX_EXCL_LOCK)
		*infomask |= HEAP_XMAX_EXCL_LOCK;
	if (infobits & XLHL_XMAX_KEYSHR_LOCK)
		*infomask |= HEAP_XMAX_KEYSHR_LOCK;
	if (infobits & XLHL_KEYS_UPDATED)
		*infomask2 |= HEAP_KEYS_UPDATED;
}

/*
 * fb_copy_page_tuple
 *    Replay helper.
 */

static HeapTuple
fb_copy_page_tuple_internal(Page page,
							BlockNumber blkno,
							OffsetNumber offnum,
							bool allow_dead)
{
	ItemId lp;
	HeapTupleData local_tuple;

	if (PageGetMaxOffsetNumber(page) < offnum)
		return NULL;

	lp = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(lp) &&
		!(allow_dead && ItemIdIsDead(lp) && ItemIdHasStorage(lp)))
		return NULL;

	MemSet(&local_tuple, 0, sizeof(local_tuple));
	local_tuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);
	local_tuple.t_len = ItemIdGetLength(lp);
	ItemPointerSet(&local_tuple.t_self, blkno, offnum);

	return heap_copytuple(&local_tuple);
}

static HeapTuple
fb_copy_page_tuple(Page page, BlockNumber blkno, OffsetNumber offnum)
{
	return fb_copy_page_tuple_internal(page, blkno, offnum, false);
}

static HeapTuple
fb_copy_page_old_tuple(Page page, BlockNumber blkno, OffsetNumber offnum)
{
	return fb_copy_page_tuple_internal(page, blkno, offnum, true);
}

/*
 * fb_record_is_speculative_insert
 *    Replay helper.
 */

static bool
fb_record_is_speculative_insert(const FbRecordRef *record)
{
	const xl_heap_insert *xlrec;

	if (record == NULL || record->kind != FB_WAL_RECORD_HEAP_INSERT ||
		record->main_data == NULL || record->main_data_len < SizeOfHeapInsert)
		return false;

	xlrec = (const xl_heap_insert *) record->main_data;
	return (xlrec->flags & XLH_INSERT_IS_SPECULATIVE) != 0;
}

/*
 * fb_record_is_super_delete
 *    Replay helper.
 */

static bool
fb_record_is_super_delete(const FbRecordRef *record)
{
	const xl_heap_delete *xlrec;

	if (record == NULL || record->kind != FB_WAL_RECORD_HEAP_DELETE ||
		record->main_data == NULL || record->main_data_len < SizeOfHeapDelete)
		return false;

	xlrec = (const xl_heap_delete *) record->main_data;
	return (xlrec->flags & XLH_DELETE_IS_SUPER) != 0;
}

/*
 * fb_page_prune_execute
 *    Replay helper.
 */

static void
fb_page_prune_execute(Page page, bool lp_truncate_only,
					  OffsetNumber *redirected, int nredirected,
					  OffsetNumber *nowdead, int ndead,
					  OffsetNumber *nowunused, int nunused)
{
	OffsetNumber *offnum;

	offnum = redirected;
	for (int i = 0; i < nredirected; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId fromlp = PageGetItemId(page, fromoff);

		ItemIdSetRedirect(fromlp, tooff);
	}

	offnum = nowdead;
	for (int i = 0; i < ndead; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId lp = PageGetItemId(page, off);

		ItemIdSetDead(lp);
	}

	offnum = nowunused;
	for (int i = 0; i < nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId lp = PageGetItemId(page, off);

		ItemIdSetUnused(lp);
	}

	if (lp_truncate_only)
		PageTruncateLinePointerArray(page);
	else
		PageRepairFragmentation(page);
}

/*
 * fb_prepare_historical_visible_tuple
 *    Replay helper.
 */

static HeapTuple
fb_prepare_historical_visible_tuple(const FbRelationInfo *info,
									FbToastStore *toast_store,
									TupleDesc tupdesc,
									HeapTuple tuple)
{
	if (tuple == NULL)
		return NULL;

	if (info != NULL && info->has_toast_locator &&
		fb_toast_tuple_uses_external(tupdesc, tuple))
		return fb_toast_rewrite_tuple(toast_store, tupdesc, tuple);

	return tuple;
}

static void
fb_finalize_row_image(const FbRelationInfo *info,
					  TupleDesc tupdesc,
					  FbToastStore *toast_store,
					  XLogRecPtr record_lsn,
					  const char *type_name,
					  const char *side_name,
					  FbRowImage *row,
					  FbReplayResult *result)
{
	HeapTuple original_tuple;

	if (row == NULL || row->tuple == NULL)
		return;

	original_tuple = row->tuple;
	PG_TRY();
	{
		row->tuple = fb_prepare_historical_visible_tuple(info, toast_store,
														 tupdesc, row->tuple);
	}
	PG_CATCH();
	{
		ErrorData  *edata = CopyErrorData();

		FlushErrorState();
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("failed to finalize toast-bearing forward row"),
				 errdetail("op=%s side=%s lsn=%X/%08X cause=%s inner_detail=%s",
						   type_name,
						   side_name,
						   LSN_FORMAT_ARGS(record_lsn),
						   edata->message ? edata->message : "unknown",
						   edata->detail ? edata->detail : "none")));
	}
	PG_END_TRY();

	if (row->tuple != original_tuple)
		fb_replay_charge_bytes(result,
							   fb_memory_heaptuple_bytes(row->tuple),
							   "forward finalized tuple");
	if (fb_toast_tuple_uses_external(tupdesc, row->tuple))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unresolved external toast datum after finalize"),
				 errdetail("op=%s side=%s lsn=%X/%08X",
						   type_name,
						   side_name,
						   LSN_FORMAT_ARGS(record_lsn))));
	row->finalized = true;
}

static bool
fb_row_image_needs_delayed_toast_finalize(const FbRelationInfo *info,
										  TupleDesc tupdesc,
										  HeapTuple tuple)
{
	if (info == NULL || tupdesc == NULL || tuple == NULL || !info->has_toast_locator)
		return false;

	return fb_toast_tuple_uses_external(tupdesc, tuple);
}

static bool
fb_row_image_needs_finalize(const FbRelationInfo *info,
							const FbRowImage *row)
{
	(void) info;

	if (row == NULL || row->tuple == NULL)
		return false;

	return !row->finalized;
}

static void
fb_pending_reverse_op_enqueue(FbPendingReverseOpQueue *queue,
							  FbReverseOp *op,
							  FbReplayResult *result)
{
	FbPendingReverseOpEntry *entry;

	if (queue == NULL || op == NULL)
		return;

	entry = palloc0(sizeof(*entry));
	entry->op = *op;
	entry->next = NULL;
	if (queue->tail != NULL)
		queue->tail->next = entry;
	else
		queue->head = entry;
	queue->tail = entry;
	fb_replay_charge_bytes(result, sizeof(*entry), "pending reverse op");
}

static void
fb_pending_reverse_op_flush(FbPendingReverseOpQueue *queue,
							const FbRelationInfo *info,
							TupleDesc tupdesc,
							FbToastStore *toast_store,
							FbReplayResult *result,
							FbReverseOpSource *source)
{
	FbPendingReverseOpEntry *entry = (queue != NULL) ? queue->head : NULL;

	while (entry != NULL)
	{
		FbPendingReverseOpEntry *next = entry->next;

		if (fb_row_image_needs_finalize(info, &entry->op.old_row))
			fb_finalize_row_image(info, tupdesc, toast_store, entry->op.record_lsn,
								  entry->op.type == FB_REVERSE_ADD ? "delete" : "update",
								  "old", &entry->op.old_row, result);

		if (fb_row_image_needs_finalize(info, &entry->op.new_row))
			fb_finalize_row_image(info, tupdesc, toast_store, entry->op.record_lsn,
								  entry->op.type == FB_REVERSE_REMOVE ? "insert" : "update",
								  "new", &entry->op.new_row, result);

		fb_reverse_source_append(source, &entry->op);
		fb_memory_release_bytes(&result->tracked_bytes, sizeof(*entry));
		pfree(entry);
		entry = next;
	}

	if (queue != NULL)
	{
		queue->head = NULL;
		queue->tail = NULL;
	}
}

/*
 * fb_replay_find_existing_block
 *    Replay helper.
 */

static FbReplayBlockState *
fb_replay_find_existing_block(FbReplayStore *store,
							  const FbRecordBlockRef *block_ref)
{
	FbReplayBlockKey key;

	MemSet(&key, 0, sizeof(key));
	key.locator = block_ref->locator;
	key.forknum = block_ref->forknum;
	key.blkno = block_ref->blkno;

	return (FbReplayBlockState *) hash_search(store->blocks, &key, HASH_FIND, NULL);
}

/*
 * fb_replay_get_block
 *    Replay helper.
 */

static FbReplayBlockState *
fb_replay_get_block(FbReplayStore *store,
					const FbRecordBlockRef *block_ref,
					FbReplayResult *result,
					bool *found)
{
	FbReplayBlockKey key;
	FbReplayBlockState *state;

	MemSet(&key, 0, sizeof(key));
	key.locator = block_ref->locator;
	key.forknum = block_ref->forknum;
	key.blkno = block_ref->blkno;

	state = (FbReplayBlockState *) hash_search(store->blocks, &key,
											   HASH_FIND, NULL);
	if (state != NULL)
	{
		if (found != NULL)
			*found = true;
		return state;
	}

	fb_replay_charge_bytes(result, sizeof(FbReplayBlockState), "BlockReplayStore");
	state = (FbReplayBlockState *) hash_search(store->blocks, &key,
											   HASH_ENTER, NULL);
	if (found != NULL)
		*found = false;
	return state;
}

/*
 * fb_replay_block_key_from_ref
 *    Replay helper.
 */

static FbReplayBlockKey
fb_replay_block_key_from_ref(const FbRecordBlockRef *block_ref)
{
	FbReplayBlockKey key;

	MemSet(&key, 0, sizeof(key));
	key.locator = block_ref->locator;
	key.forknum = block_ref->forknum;
	key.blkno = block_ref->blkno;
	return key;
}

static HTAB *
fb_replay_create_future_block_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(FbReplayBlockKey);
	ctl.entrysize = sizeof(FbReplayFutureBlockEntry);
	return hash_create("fb replay future block records",
					   1024,
					   &ctl,
					   HASH_ELEM | HASH_BLOBS);
}

static HTAB *
fb_replay_create_prune_lookahead_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(FbReplayPruneLookaheadEntry);
	return hash_create("fb replay prune lookahead",
					   1024,
					   &ctl,
					   HASH_ELEM | HASH_BLOBS);
}

/*
 * fb_replay_note_missing_block
 *    Replay helper.
 */

static void
fb_replay_note_missing_block(FbReplayControl *control,
							 const FbRecordBlockRef *block_ref)
{
	FbReplayBlockKey key;
	FbReplayMissingBlock *entry;
	bool found;

	if (control == NULL || control->missing_blocks == NULL)
		return;

	key = fb_replay_block_key_from_ref(block_ref);
	entry = (FbReplayMissingBlock *) hash_search(control->missing_blocks,
												 &key, HASH_ENTER, &found);
	if (!found)
	{
		MemSet(entry, 0, sizeof(*entry));
		entry->key = key;
		entry->first_record_index = control->record_index;
		entry->first_record_lsn = InvalidXLogRecPtr;
	}
	else if (control->record_index < entry->first_record_index)
		entry->first_record_index = control->record_index;
}

static void
fb_replay_note_record_missing_blocks(FbReplayControl *control,
									 const FbRecordRef *record)
{
	int	block_index;

	if (control == NULL || record == NULL)
		return;

	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		fb_replay_note_missing_block(control, &record->blocks[block_index]);
		if (control->missing_blocks != NULL)
		{
			FbReplayMissingBlock *entry;

			entry = fb_replay_find_missing_block(control->missing_blocks,
												 &record->blocks[block_index]);
			if (entry != NULL &&
				(XLogRecPtrIsInvalid(entry->first_record_lsn) ||
				 record->lsn < entry->first_record_lsn))
				entry->first_record_lsn = record->lsn;
		}
	}
}

static bool
fb_replay_record_block_has_wal_id(const FbRecordRef *record,
								  int block_index,
								  uint8 wal_block_id)
{
	const FbRecordBlockRef *block_ref;

	if (record == NULL ||
		block_index < 0 ||
		block_index >= record->block_count)
		return false;

	block_ref = &record->blocks[block_index];
	return block_ref->in_use && block_ref->block_id == wal_block_id;
}

/*
 * fb_record_allows_init_for_block
 *    Replay helper.
 */

static bool
fb_record_allows_init_for_block(const FbRecordRef *record, int block_index)
{
	if (record == NULL || !record->init_page)
		return false;

	switch (record->kind)
	{
		case FB_WAL_RECORD_HEAP_INSERT:
		case FB_WAL_RECORD_HEAP_UPDATE:
		case FB_WAL_RECORD_HEAP_HOT_UPDATE:
		case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
			return fb_replay_record_block_has_wal_id(record, block_index, 0);
		default:
			return false;
	}
}

static FbRecordBlockRef *
fb_replay_find_block_ref_by_wal_id(FbRecordRef *record, uint8 wal_block_id)
{
	int i;

	if (record == NULL)
		return NULL;

	for (i = 0; i < record->block_count; i++)
	{
		FbRecordBlockRef *block_ref = &record->blocks[i];

		if (block_ref->in_use && block_ref->block_id == wal_block_id)
			return block_ref;
	}

	return NULL;
}

/*
 * fb_replay_find_missing_block
 *    Replay helper.
 */

static FbReplayMissingBlock *
fb_replay_find_missing_block(HTAB *missing_blocks,
							 const FbRecordBlockRef *block_ref)
{
	FbReplayBlockKey key;

	if (missing_blocks == NULL)
		return NULL;

	key = fb_replay_block_key_from_ref(block_ref);
	return (FbReplayMissingBlock *) hash_search(missing_blocks, &key,
												HASH_FIND, NULL);
}

static bool
fb_replay_record_touches_missing_block(FbReplayControl *control,
									   const FbRecordRef *record)
{
	int	block_index;

	if (control == NULL || record == NULL || control->missing_blocks == NULL)
		return false;

	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		if (fb_replay_find_missing_block(control->missing_blocks,
										 &record->blocks[block_index]) != NULL)
			return true;
	}

	return false;
}

/*
 * fb_replay_missing_block_count
 *    Replay helper.
 */

static uint32
fb_replay_missing_block_count(HTAB *missing_blocks)
{
	HASH_SEQ_STATUS seq;
	FbReplayMissingBlock *entry;
	uint32 count = 0;

	if (missing_blocks == NULL)
		return 0;

	hash_seq_init(&seq, missing_blocks);
	while ((entry = (FbReplayMissingBlock *) hash_seq_search(&seq)) != NULL)
		count++;

	return count;
}

static XLogRecPtr
fb_replay_missing_blocks_end_lsn(HTAB *missing_blocks)
{
	HASH_SEQ_STATUS seq;
	FbReplayMissingBlock *entry;
	XLogRecPtr end_lsn = InvalidXLogRecPtr;

	if (missing_blocks == NULL)
		return end_lsn;

	hash_seq_init(&seq, missing_blocks);
	while ((entry = (FbReplayMissingBlock *) hash_seq_search(&seq)) != NULL)
	{
		if (XLogRecPtrIsInvalid(entry->first_record_lsn))
			continue;
		if (XLogRecPtrIsInvalid(end_lsn) || entry->first_record_lsn > end_lsn)
			end_lsn = entry->first_record_lsn;
	}

	return end_lsn;
}

/*
 * fb_replay_merge_missing_blocks
 *    Replay helper.
 */

static void
fb_replay_merge_missing_blocks(HTAB *dest, HTAB *src)
{
	HASH_SEQ_STATUS seq;
	FbReplayMissingBlock *entry;

	hash_seq_init(&seq, src);
	while ((entry = (FbReplayMissingBlock *) hash_seq_search(&seq)) != NULL)
	{
		FbReplayMissingBlock *dest_entry;
		bool found;

		dest_entry = (FbReplayMissingBlock *) hash_search(dest, &entry->key,
													 HASH_ENTER, &found);
		if (!found)
			*dest_entry = *entry;
		else
		{
			if (entry->anchor_found &&
				(!dest_entry->anchor_found ||
				 entry->anchor_lsn < dest_entry->anchor_lsn))
			{
				dest_entry->anchor_lsn = entry->anchor_lsn;
				dest_entry->anchor_found = entry->anchor_found;
			}
			if (entry->first_record_index < dest_entry->first_record_index)
				dest_entry->first_record_index = entry->first_record_index;
			if (XLogRecPtrIsInvalid(dest_entry->first_record_lsn) ||
				(!XLogRecPtrIsInvalid(entry->first_record_lsn) &&
				 entry->first_record_lsn < dest_entry->first_record_lsn))
				dest_entry->first_record_lsn = entry->first_record_lsn;
		}
	}
}

static bool
fb_replay_should_apply_raw_backtrack(const FbRecordRef *record,
									 HTAB *missing_blocks)
{
	int block_index;

	if (record == NULL || missing_blocks == NULL)
		return false;

	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		const FbRecordBlockRef *block_ref = &record->blocks[block_index];
		FbReplayMissingBlock *entry;

		entry = fb_replay_find_missing_block(missing_blocks, block_ref);
		if (entry == NULL ||
			!entry->anchor_found ||
			XLogRecPtrIsInvalid(entry->anchor_lsn) ||
			XLogRecPtrIsInvalid(entry->first_record_lsn))
			continue;
		if (record->lsn < entry->anchor_lsn ||
			record->lsn >= entry->first_record_lsn)
			continue;


		return true;
	}

	return false;
}

static bool
fb_replay_raw_warm_visitor(XLogReaderState *reader, void *arg)
{
	FbReplayRawWarmVisitorState *state = (FbReplayRawWarmVisitorState *) arg;
	FbRecordRef record;

	if (reader == NULL || state == NULL || state->info == NULL ||
		state->store == NULL || state->apply_blocks == NULL || state->result == NULL)
		return true;

	if (!fb_wal_decode_record_ref(reader, state->info, &record))
		return true;
	if (fb_replay_should_apply_raw_backtrack(&record,
											 state->apply_blocks))
	{
		fb_replay_apply_record(state->info,
							   &record,
							   state->store,
							   NULL,
							   state->toast_tupdesc,
							   state->toast_store,
							   state->control,
							   state->result,
							   NULL,
							   NULL);
		if (state->control != NULL &&
			state->control->missing_blocks != NULL &&
			fb_replay_missing_block_count(state->control->missing_blocks) > 0)
		{
			fb_wal_release_record(&record);
			return false;
		}
	}
	fb_wal_release_record(&record);

	return true;
}

static bool
fb_replay_raw_anchor_visitor(XLogReaderState *reader, void *arg)
{
	FbReplayRawAnchorVisitorState *state = (FbReplayRawAnchorVisitorState *) arg;
	FbRecordRef record;
	int block_index;

	if (state == NULL || state->info == NULL || state->missing_blocks == NULL)
		return true;
	if (!fb_wal_decode_record_ref(reader, state->info, &record))
		return true;

	for (block_index = 0; block_index < record.block_count; block_index++)
	{
		const FbRecordBlockRef *block_ref = &record.blocks[block_index];
		FbReplayMissingBlock *entry;
		bool materializes;

		entry = fb_replay_find_missing_block(state->missing_blocks, block_ref);
		if (entry == NULL ||
			XLogRecPtrIsInvalid(entry->first_record_lsn) ||
			record.lsn >= entry->first_record_lsn)
			continue;

		materializes =
			fb_replay_record_can_anchor_block(&record, block_index);
		if (!materializes)
			continue;

		if (!entry->anchor_found && state->unresolved > 0)
			state->unresolved--;
		entry->anchor_found = true;
		entry->anchor_lsn = record.lsn;
	}

	fb_wal_release_record(&record);
	return state->unresolved > 0;
}

/*
 * fb_replay_resolve_missing_anchors
 *    Replay helper.
 */

static void
fb_replay_resolve_missing_anchors(const FbRelationInfo *info,
								  const FbWalRecordIndex *index,
								  HTAB *missing_blocks,
								  FbReplayResult *result)
{
	uint32 unresolved;
	uint32 segment_index;
	FbWalRecordCursor *cursor;
	FbRecordRef record;
	uint32 record_index = 0;

	unresolved = fb_replay_missing_block_count(missing_blocks);
	if (unresolved == 0)
		return;

	if (index != NULL &&
		index->resolved_segments != NULL &&
		index->resolved_segment_count > 0 &&
		index->summary_cache != NULL)
	{
		for (segment_index = index->resolved_segment_count; segment_index > 0 && unresolved > 0; segment_index--)
		{
			const FbWalResolvedSegment *segment = &index->resolved_segments[segment_index - 1];
			FbSummaryBlockAnchor *anchors = NULL;
			uint32 anchor_count = 0;
			uint32 anchor_index;

			if (!fb_summary_segment_lookup_block_anchors_cached(segment->path,
																segment->bytes,
																segment->timeline_id,
																segment->segno,
																index->wal_seg_size,
																segment->source_kind,
																info,
																index->summary_cache,
																&anchors,
																&anchor_count))
				continue;
			if (result != NULL)
				result->summary_anchor_segments_read++;

			for (anchor_index = anchor_count; anchor_index > 0 && unresolved > 0; anchor_index--)
			{
				const FbSummaryBlockAnchor *anchor = &anchors[anchor_index - 1];
				FbRecordBlockRef block_ref;
				FbReplayMissingBlock *entry;

				MemSet(&block_ref, 0, sizeof(block_ref));
				block_ref.in_use = true;
				block_ref.locator = anchor->locator;
				block_ref.forknum = anchor->forknum;
				block_ref.blkno = anchor->blkno;

				entry = fb_replay_find_missing_block(missing_blocks, &block_ref);
				if (entry == NULL || entry->anchor_found ||
					XLogRecPtrIsInvalid(entry->first_record_lsn) ||
					anchor->anchor_lsn >= entry->first_record_lsn)
					continue;

				entry->anchor_found = true;
				entry->anchor_lsn = anchor->anchor_lsn;
				if (result != NULL)
					result->summary_anchor_hits++;
				unresolved--;
			}

			if (anchors != NULL)
				pfree(anchors);
		}
	}

	if (unresolved == 0)
		return;
	if (result != NULL)
		result->summary_anchor_fallback += unresolved;

	cursor = fb_wal_record_cursor_open(index, FB_SPOOL_BACKWARD);
	while (unresolved > 0 &&
		   fb_wal_record_cursor_read_skeleton(cursor, &record, &record_index))
	{
		int block_index;

		for (block_index = 0; block_index < record.block_count; block_index++)
		{
			const FbRecordBlockRef *block_ref = &record.blocks[block_index];
			FbReplayMissingBlock *entry;

			entry = fb_replay_find_missing_block(missing_blocks, block_ref);
			if (entry == NULL || entry->anchor_found ||
				record_index >= entry->first_record_index)
				continue;

			if (!fb_record_block_has_applicable_image(block_ref) &&
				!fb_record_allows_init_for_block(&record, block_index))
				continue;

			entry->anchor_found = true;
			entry->anchor_lsn = record.lsn;
			unresolved--;
		}
	}
	fb_wal_record_cursor_close(cursor);

	if (unresolved > 0 &&
		index != NULL &&
		!XLogRecPtrIsInvalid(index->anchor_redo_lsn))
	{
		HASH_SEQ_STATUS seq;
		FbReplayMissingBlock *entry;
		XLogRecPtr raw_end_lsn = InvalidXLogRecPtr;
		FbReplayRawAnchorVisitorState raw_state;

		hash_seq_init(&seq, missing_blocks);
		while ((entry = (FbReplayMissingBlock *) hash_seq_search(&seq)) != NULL)
		{
			if (entry->anchor_found || XLogRecPtrIsInvalid(entry->first_record_lsn))
				continue;
			if (XLogRecPtrIsInvalid(raw_end_lsn) ||
				entry->first_record_lsn > raw_end_lsn)
				raw_end_lsn = entry->first_record_lsn;
		}

		if (!XLogRecPtrIsInvalid(raw_end_lsn) && raw_end_lsn > index->anchor_redo_lsn)
		{
			MemSet(&raw_state, 0, sizeof(raw_state));
			raw_state.info = info;
			raw_state.missing_blocks = missing_blocks;
			raw_state.unresolved = unresolved;
			fb_wal_visit_resolved_records(index,
										  index->anchor_redo_lsn,
										  raw_end_lsn,
										  fb_replay_raw_anchor_visitor,
										  &raw_state);
			unresolved = raw_state.unresolved;
		}
	}

}

/*
 * fb_replay_min_anchor_lsn
 *    Replay helper.
 */

static XLogRecPtr
fb_replay_min_anchor_lsn(HTAB *missing_blocks)
{
	HASH_SEQ_STATUS seq;
	FbReplayMissingBlock *entry;
	XLogRecPtr min_lsn = InvalidXLogRecPtr;

	hash_seq_init(&seq, missing_blocks);
	while ((entry = (FbReplayMissingBlock *) hash_seq_search(&seq)) != NULL)
	{
		if (!entry->anchor_found)
			continue;
		if (XLogRecPtrIsInvalid(min_lsn) || entry->anchor_lsn < min_lsn)
			min_lsn = entry->anchor_lsn;
	}

	return min_lsn;
}

/*
 * fb_replay_raise_unresolved_missing_fpi
 *    Replay helper.
 */

static void
fb_replay_raise_unresolved_missing_fpi(const FbWalRecordIndex *index,
									   HTAB *missing_blocks)
{
	HASH_SEQ_STATUS seq;
	FbReplayMissingBlock *entry;

	hash_seq_init(&seq, missing_blocks);
	while ((entry = (FbReplayMissingBlock *) hash_seq_search(&seq)) != NULL)
	{
		if (!entry->anchor_found)
		{
			const FbRecordBlockRef temp_block_ref = {
				.in_use = true,
				.locator = entry->key.locator,
				.forknum = entry->key.forknum,
				.blkno = entry->key.blkno
			};
			FbRecordRef record;

			if (!fb_wal_record_load(index, entry->first_record_index, &record))
				elog(ERROR, "missing record %u while raising unresolved missing FPI",
					 entry->first_record_index);
			fb_replay_missing_fpi_error(&record, &temp_block_ref, false);
		}
	}
}

/*
 * fb_record_should_apply_for_backtrack
 *    Replay helper.
 */

static bool
fb_record_should_apply_for_backtrack(const FbRecordRef *record,
									 HTAB *missing_blocks)
{
	bool touches_backtracked = false;
	int block_index;

	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		const FbRecordBlockRef *block_ref = &record->blocks[block_index];
		FbReplayMissingBlock *entry;

		entry = fb_replay_find_missing_block(missing_blocks, block_ref);
		if (entry == NULL)
			continue;

		touches_backtracked = true;
		if (!entry->anchor_found ||
			XLogRecPtrIsInvalid(entry->anchor_lsn) ||
			record->lsn < entry->anchor_lsn)
			return false;
	}

	return touches_backtracked;
}

/*
 * fb_replay_missing_fpi_error
 *    Replay helper.
 */

static void
fb_replay_missing_fpi_error(const FbRecordRef *record,
							const FbRecordBlockRef *block_ref,
							bool allow_init)
{
	const char *kind = "unknown";

	if (record != NULL)
	{
		switch (record->kind)
		{
			case FB_WAL_RECORD_HEAP_INSERT:
				kind = "heap_insert";
				break;
			case FB_WAL_RECORD_HEAP_DELETE:
				kind = "heap_delete";
				break;
			case FB_WAL_RECORD_HEAP_UPDATE:
				kind = "heap_update";
				break;
			case FB_WAL_RECORD_HEAP_HOT_UPDATE:
				kind = "heap_hot_update";
				break;
			case FB_WAL_RECORD_HEAP_CONFIRM:
				kind = "heap_confirm";
				break;
			case FB_WAL_RECORD_HEAP_LOCK:
				kind = "heap_lock";
				break;
			case FB_WAL_RECORD_HEAP_INPLACE:
				kind = "heap_inplace";
				break;
			case FB_WAL_RECORD_HEAP2_PRUNE:
				kind = "heap2_prune";
				break;
			case FB_WAL_RECORD_HEAP2_VISIBLE:
				kind = "heap2_visible";
				break;
			case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
				kind = "heap2_multi_insert";
				break;
			case FB_WAL_RECORD_HEAP2_LOCK_UPDATED:
				kind = "heap2_lock_updated";
				break;
			case FB_WAL_RECORD_XLOG_FPI:
				kind = "xlog_fpi";
				break;
			case FB_WAL_RECORD_XLOG_FPI_FOR_HINT:
				kind = "xlog_fpi_for_hint";
				break;
			default:
				break;
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("missing FPI for block %u", block_ref->blkno),
			 errdetail("kind=%s lsn=%X/%08X rel=%u/%u/%u fork=%u blk=%u allow_init=%s has_image=%s apply_image=%s has_data=%s init_page=%s toast=%s",
					   kind,
					   LSN_FORMAT_ARGS(record->lsn),
					   FB_LOCATOR_SPCOID(block_ref->locator),
					   FB_LOCATOR_DBOID(block_ref->locator),
					   FB_LOCATOR_RELNUMBER(block_ref->locator),
					   (unsigned int) block_ref->forknum,
					   block_ref->blkno,
					   allow_init ? "true" : "false",
					   block_ref->has_image ? "true" : "false",
					   block_ref->apply_image ? "true" : "false",
					   block_ref->has_data ? "true" : "false",
					   record->init_page ? "true" : "false",
					   block_ref->is_toast_relation ? "true" : "false")));
}

/*
 * fb_replay_ensure_block_ready
 *    Replay helper.
 */

static void
fb_replay_ensure_block_ready(FbReplayStore *store,
							 const FbRecordBlockRef *block_ref,
							 const FbRecordRef *record,
							 bool allow_init,
							 FbReplayControl *control,
							 FbReplayResult *result,
							 FbReplayBlockState **state_out,
							 bool *ready)
{
	FbReplayBlockState *state;
	bool found;

	if (ready != NULL)
		*ready = false;

	state = fb_replay_get_block(store, block_ref, result, &found);
	if (!found)
	{
		MemSet(state, 0, sizeof(*state));
		state->key.locator = block_ref->locator;
		state->key.forknum = block_ref->forknum;
		state->key.blkno = block_ref->blkno;
	}

	if (fb_record_block_has_applicable_image(block_ref) &&
		block_ref->image != NULL)
	{
		memcpy(state->page, block_ref->image, BLCKSZ);
		state->initialized = true;
		result->blocks_materialized++;
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395737 &&
			block_ref->blkno == 38724)
			fb_replay_debug_log_documents38724_page("ensure image", record->lsn,
													 (Page) state->page);
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
			block_ref->blkno == 17079)
			elog(WARNING,
				 "fb_debug toast ensure blk=%u lsn=%X/%08X materialized=image maxoff=%u lower=%u upper=%u exact_free=%u heap_free=%u",
				 block_ref->blkno,
				 LSN_FORMAT_ARGS(record->lsn),
				 (unsigned int) PageGetMaxOffsetNumber((Page) state->page),
				 (unsigned int) ((PageHeader) state->page)->pd_lower,
				 (unsigned int) ((PageHeader) state->page)->pd_upper,
				 (unsigned int) PageGetExactFreeSpace((Page) state->page),
				 (unsigned int) PageGetHeapFreeSpace((Page) state->page));
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
			block_ref->blkno == 26273)
			fb_replay_debug_log_toast26273_page("ensure image", record->lsn,
												  (Page) state->page);
	}
	else if (allow_init)
	{
		PageInit((Page) state->page, BLCKSZ, 0);
		state->initialized = true;
		result->blocks_materialized++;
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
			block_ref->blkno == 17079)
			elog(WARNING,
				 "fb_debug toast ensure blk=%u lsn=%X/%08X materialized=init maxoff=%u lower=%u upper=%u exact_free=%u heap_free=%u",
				 block_ref->blkno,
				 LSN_FORMAT_ARGS(record->lsn),
				 (unsigned int) PageGetMaxOffsetNumber((Page) state->page),
				 (unsigned int) ((PageHeader) state->page)->pd_lower,
				 (unsigned int) ((PageHeader) state->page)->pd_upper,
				 (unsigned int) PageGetExactFreeSpace((Page) state->page),
				 (unsigned int) PageGetHeapFreeSpace((Page) state->page));
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
			block_ref->blkno == 26273)
			fb_replay_debug_log_toast26273_page("ensure init", record->lsn,
												 (Page) state->page);
	}

	if (!state->initialized)
	{
		if (control != NULL && control->phase == FB_REPLAY_PHASE_DISCOVER)
		{
			fb_replay_note_missing_block(control, block_ref);
			return;
		}

		result->replay_errors++;
		fb_replay_missing_fpi_error(record, block_ref, allow_init);
	}

	if (state_out != NULL)
		*state_out = state;
	if (ready != NULL)
		*ready = true;
}

/*
 * fb_append_forward_insert
 *    Replay helper.
 */

static void
fb_append_forward_insert(const FbRelationInfo *info, TupleDesc tupdesc,
						 FbToastStore *toast_store,
						 const FbRecordRef *record, HeapTuple new_tuple,
						 FbReplayResult *result,
						 FbReverseOpSource *source,
						 FbPendingReverseOpQueue *pending_ops)
{
	FbReverseOp op;

	MemSet(&op, 0, sizeof(op));
	op.type = FB_REVERSE_REMOVE;
	op.xid = record->xid;
	op.commit_ts = record->commit_ts;
	op.commit_lsn = record->commit_lsn;
	op.record_lsn = record->lsn;
	op.new_row.tuple = new_tuple;
	fb_replay_charge_bytes(result,
						   fb_memory_heaptuple_bytes(new_tuple),
						   "forward row tuple");
	if (fb_row_image_needs_delayed_toast_finalize(info, tupdesc, new_tuple))
	{
		fb_pending_reverse_op_enqueue(pending_ops, &op, result);
		return;
	}
	fb_finalize_row_image(info, tupdesc, toast_store, record->lsn,
						  "insert", "new", &op.new_row, result);
	fb_reverse_source_append(source, &op);
}

/*
 * fb_append_forward_delete
 *    Replay helper.
 */

static void
fb_append_forward_delete(const FbRelationInfo *info, TupleDesc tupdesc,
						 FbToastStore *toast_store,
						 const FbRecordRef *record, HeapTuple old_tuple,
						 FbReplayResult *result,
						 FbReverseOpSource *source,
						 FbPendingReverseOpQueue *pending_ops)
{
	FbReverseOp op;

	MemSet(&op, 0, sizeof(op));
	op.type = FB_REVERSE_ADD;
	op.xid = record->xid;
	op.commit_ts = record->commit_ts;
	op.commit_lsn = record->commit_lsn;
	op.record_lsn = record->lsn;
	op.old_row.tuple = old_tuple;
	fb_replay_charge_bytes(result,
						   fb_memory_heaptuple_bytes(old_tuple),
						   "forward row tuple");
	if (fb_row_image_needs_delayed_toast_finalize(info, tupdesc, old_tuple))
	{
		fb_pending_reverse_op_enqueue(pending_ops, &op, result);
		return;
	}
	fb_finalize_row_image(info, tupdesc, toast_store, record->lsn,
						  "delete", "old", &op.old_row, result);
	fb_reverse_source_append(source, &op);
}

/*
 * fb_append_forward_update
 *    Replay helper.
 */

static void
fb_append_forward_update(const FbRelationInfo *info, TupleDesc tupdesc,
						 FbToastStore *toast_store,
						 const FbRecordRef *record,
						 HeapTuple old_tuple, HeapTuple new_tuple,
						 FbReplayResult *result,
						 FbReverseOpSource *source,
						 FbPendingReverseOpQueue *pending_ops)
{
	FbReverseOp op;

	MemSet(&op, 0, sizeof(op));
	op.type = FB_REVERSE_REPLACE;
	op.xid = record->xid;
	op.commit_ts = record->commit_ts;
	op.commit_lsn = record->commit_lsn;
	op.record_lsn = record->lsn;
	op.old_row.tuple = old_tuple;
	op.new_row.tuple = new_tuple;
	fb_replay_charge_bytes(result,
						   fb_memory_heaptuple_bytes(old_tuple),
						   "forward row tuple");
	fb_replay_charge_bytes(result,
						   fb_memory_heaptuple_bytes(new_tuple),
						   "forward row tuple");
	if (fb_row_image_needs_delayed_toast_finalize(info, tupdesc, old_tuple) ||
		fb_row_image_needs_delayed_toast_finalize(info, tupdesc, new_tuple))
	{
		fb_pending_reverse_op_enqueue(pending_ops, &op, result);
		return;
	}
	fb_finalize_row_image(info, tupdesc, toast_store, record->lsn,
						  "update", "old", &op.old_row, result);
	fb_finalize_row_image(info, tupdesc, toast_store, record->lsn,
						  "update", "new", &op.new_row, result);
	fb_reverse_source_append(source, &op);
}

/*
 * fb_toast_store_put_from_tuple
 *    Replay helper.
 */

static void
fb_toast_store_put_from_tuple(FbToastStore *toast_store,
							  TupleDesc toast_tupdesc,
							  HeapTuple tuple)
{
	if (toast_store == NULL || toast_tupdesc == NULL || tuple == NULL)
		return;

	fb_toast_store_put_tuple(toast_store, toast_tupdesc, tuple);
}

/*
 * fb_toast_store_remove_from_tuple
 *    Replay helper.
 */

static void
fb_toast_store_remove_from_tuple(FbToastStore *toast_store,
								 TupleDesc toast_tupdesc,
								 HeapTuple tuple)
{
	if (toast_store == NULL || toast_tupdesc == NULL || tuple == NULL)
		return;

	fb_toast_store_remove_tuple(toast_store, toast_tupdesc, tuple);
}

/*
 * fb_replay_sync_toast_page_if_image
 *    Replay helper.
 */

static void
fb_replay_sync_toast_page_if_image(const FbRecordBlockRef *block_ref,
								   Page page,
								   TupleDesc toast_tupdesc,
								   FbToastStore *toast_store)
{
	if (block_ref == NULL || page == NULL ||
		!block_ref->is_toast_relation ||
		!block_ref->has_image ||
		toast_store == NULL ||
		toast_tupdesc == NULL)
		return;

	fb_toast_store_sync_page(toast_store, toast_tupdesc, page, block_ref->blkno);
}

static bool
fb_replay_block_needs_redo(const FbRecordBlockRef *block_ref)
{
	if (block_ref == NULL)
		return false;

	return !fb_record_block_has_applicable_image(block_ref);
}

/*
 * fb_replay_get_tuple_from_page
 *    Replay helper.
 */

static HeapTupleHeader
fb_replay_get_tuple_from_page_internal(Page page,
									   OffsetNumber offnum,
									   uint32 *tuple_len,
									   bool allow_dead)
{
	ItemId lp;

	if (PageGetMaxOffsetNumber(page) < offnum)
		return NULL;

	lp = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(lp) &&
		!(allow_dead && ItemIdIsDead(lp) && ItemIdHasStorage(lp)))
		return NULL;

	if (tuple_len != NULL)
		*tuple_len = ItemIdGetLength(lp);
	return (HeapTupleHeader) PageGetItem(page, lp);
}

static HeapTupleHeader
fb_replay_get_tuple_from_page(Page page, OffsetNumber offnum, uint32 *tuple_len)
{
	return fb_replay_get_tuple_from_page_internal(page, offnum, tuple_len, false);
}

static HeapTupleHeader
fb_replay_get_old_tuple_from_page(Page page, OffsetNumber offnum, uint32 *tuple_len)
{
	return fb_replay_get_tuple_from_page_internal(page, offnum, tuple_len, true);
}

static const char *
fb_replay_debug_lp_state(Page page, OffsetNumber offnum)
{
	ItemId lp;

	if (page == NULL)
		return "no-page";
	if (offnum < FirstOffsetNumber || PageGetMaxOffsetNumber(page) < offnum)
		return "out-of-range";

	lp = PageGetItemId(page, offnum);
	if (ItemIdIsNormal(lp))
		return "normal";
	if (ItemIdIsRedirected(lp))
		return "redirect";
	if (ItemIdIsDead(lp))
		return "dead";
	if (ItemIdIsUsed(lp))
		return "used";
	return "unused";
}

static void
fb_replay_debug_log_toast17079_page(const char *label,
									 XLogRecPtr lsn,
									 Page page)
{
	if (page == NULL)
		return;

	elog(WARNING,
		 "fb_debug toast %s blk=17079 lsn=%X/%08X maxoff=%u lower=%u upper=%u exact_free=%u heap_free=%u lp23=%s lp24=%s lp41=%s lp42=%s",
		 label,
		 LSN_FORMAT_ARGS(lsn),
		 (unsigned int) PageGetMaxOffsetNumber(page),
		 (unsigned int) ((PageHeader) page)->pd_lower,
		 (unsigned int) ((PageHeader) page)->pd_upper,
		 (unsigned int) PageGetExactFreeSpace(page),
		 (unsigned int) PageGetHeapFreeSpace(page),
		 fb_replay_debug_lp_state(page, 23),
		 fb_replay_debug_lp_state(page, 24),
		 fb_replay_debug_lp_state(page, 41),
		 fb_replay_debug_lp_state(page, 42));
}

static void
fb_replay_debug_log_toast26273_page(const char *label,
									 XLogRecPtr lsn,
									 Page page)
{
	if (page == NULL)
		return;

	elog(WARNING,
		 "fb_debug toast %s blk=26273 lsn=%X/%08X maxoff=%u lower=%u upper=%u exact_free=%u heap_free=%u lp27=%s lp28=%s lp41=%s lp42=%s",
		 label,
		 LSN_FORMAT_ARGS(lsn),
		 (unsigned int) PageGetMaxOffsetNumber(page),
		 (unsigned int) ((PageHeader) page)->pd_lower,
		 (unsigned int) ((PageHeader) page)->pd_upper,
		 (unsigned int) PageGetExactFreeSpace(page),
		 (unsigned int) PageGetHeapFreeSpace(page),
		 fb_replay_debug_lp_state(page, 27),
		 fb_replay_debug_lp_state(page, 28),
		 fb_replay_debug_lp_state(page, 41),
		 fb_replay_debug_lp_state(page, 42));
}

static void
fb_replay_debug_log_documents38724_page(const char *label,
										  XLogRecPtr lsn,
										  Page page)
{
	if (page == NULL)
		return;

	elog(WARNING,
		 "fb_debug documents %s blk=38724 lsn=%X/%08X maxoff=%u lower=%u upper=%u exact_free=%u heap_free=%u lp1=%s lp2=%s lp3=%s lp4=%s",
		 label,
		 LSN_FORMAT_ARGS(lsn),
		 (unsigned int) PageGetMaxOffsetNumber(page),
		 (unsigned int) ((PageHeader) page)->pd_lower,
		 (unsigned int) ((PageHeader) page)->pd_upper,
		 (unsigned int) PageGetExactFreeSpace(page),
		 (unsigned int) PageGetHeapFreeSpace(page),
		 fb_replay_debug_lp_state(page, 1),
		 fb_replay_debug_lp_state(page, 2),
		 fb_replay_debug_lp_state(page, 3),
		 fb_replay_debug_lp_state(page, 4));
}

static bool
fb_replay_block_ref_matches_key(const FbRecordBlockRef *block_ref,
								const FbReplayBlockKey *key)
{
	return block_ref != NULL &&
		key != NULL &&
		block_ref->in_use &&
		RelFileLocatorEquals(block_ref->locator, key->locator) &&
		block_ref->forknum == key->forknum &&
		block_ref->blkno == key->blkno;
}

static bool
fb_replay_page_has_insert_room(Page page, OffsetNumber offnum)
{
	if (page == NULL || offnum < FirstOffsetNumber)
		return false;

	if (PageGetMaxOffsetNumber(page) < offnum)
		return PageGetMaxOffsetNumber(page) + 1 >= offnum;

	return !ItemIdIsUsed(PageGetItemId(page, offnum));
}

static bool
fb_replay_expand_unused_slots(Page page, OffsetNumber offnum)
{
	PageHeader pagehdr = (PageHeader) page;
	OffsetNumber maxoff;

	if (page == NULL || offnum < FirstOffsetNumber)
		return false;

	maxoff = PageGetMaxOffsetNumber(page);
	while (maxoff + 1 < offnum)
	{
		ItemId lp;

		if (pagehdr->pd_lower + sizeof(ItemIdData) > pagehdr->pd_upper)
			return false;
		pagehdr->pd_lower += sizeof(ItemIdData);
		maxoff++;
		lp = PageGetItemId(page, maxoff);
		ItemIdSetUnused(lp);
	}

	return true;
}

static bool
fb_replay_future_block_record_compose(const FbRecordRef *record,
									  int block_index,
									  const FbReplayFutureBlockRecord *after,
									  FbReplayFutureBlockRecord *before)
{
	const FbReplayFutureBlockRecord *source = after;
	FbReplayFutureBlockRecord local_after;

	if (after != NULL && before == after)
	{
		local_after = *after;
		source = &local_after;
	}
	if (before != NULL)
		MemSet(before, 0, sizeof(*before));
	if (record == NULL || before == NULL ||
		block_index < 0 || block_index >= record->block_count)
		return false;
	if (source != NULL && source->valid && !source->materializes_page)
	{
		before->valid = true;
		before->old_tuple_offsets = (source == &local_after) ?
			local_after.old_tuple_offsets :
			bms_copy(source->old_tuple_offsets);
		before->new_tuple_slot_offsets = (source == &local_after) ?
			local_after.new_tuple_slot_offsets :
			bms_copy(source->new_tuple_slot_offsets);
	}

	switch (record->kind)
	{
		case FB_WAL_RECORD_HEAP_INSERT:
		{
			xl_heap_insert *xlrec = (xl_heap_insert *) record->main_data;

			if (fb_wal_record_block_materializes_page(record, block_index))
			{
				fb_replay_future_block_record_set_materializes(before);
				return true;
			}

			fb_replay_future_block_record_remove_old(before, xlrec->offnum);
			fb_replay_future_block_record_add_new(before, xlrec->offnum);
			return true;
		}
		case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
		{
			xl_heap_multi_insert *xlrec =
				(xl_heap_multi_insert *) record->main_data;

			if (fb_wal_record_block_materializes_page(record, block_index))
			{
				fb_replay_future_block_record_set_materializes(before);
				return true;
			}

			for (int tuple_index = 0; tuple_index < xlrec->ntuples; tuple_index++)
			{
				OffsetNumber offnum;

				if (record->init_page)
					offnum = FirstOffsetNumber + tuple_index;
				else
					offnum = xlrec->offsets[tuple_index];
				fb_replay_future_block_record_remove_old(before, offnum);
				fb_replay_future_block_record_add_new(before, offnum);
			}
			return true;
		}
		case FB_WAL_RECORD_HEAP_UPDATE:
		case FB_WAL_RECORD_HEAP_HOT_UPDATE:
		{
			xl_heap_update *xlrec = (xl_heap_update *) record->main_data;
			bool image_applied =
				fb_record_block_has_applicable_image(&record->blocks[block_index]);
			bool is_new_block =
				fb_replay_record_block_has_wal_id(record, block_index, 0);
			bool is_old_block =
				fb_replay_record_block_has_wal_id(record, block_index, 1);

			if (is_new_block || is_old_block)
			{
				if (is_new_block)
				{
					if (fb_wal_record_block_materializes_page(record, block_index))
					{
						fb_replay_future_block_record_set_materializes(before);
						return true;
					}

					fb_replay_future_block_record_remove_old(before,
															 xlrec->new_offnum);
					fb_replay_future_block_record_add_new(before,
														  xlrec->new_offnum);
				}
				else if (!image_applied)
					fb_replay_future_block_record_add_old(before,
														  xlrec->old_offnum);
			}
			else
			{
				if (image_applied)
				{
					fb_replay_future_block_record_set_materializes(before);
					return true;
				}

				fb_replay_future_block_record_remove_old(before, xlrec->new_offnum);
				fb_replay_future_block_record_add_old(before, xlrec->old_offnum);
				fb_replay_future_block_record_add_new(before, xlrec->new_offnum);
			}
			return true;
		}
		case FB_WAL_RECORD_HEAP_DELETE:
		{
			xl_heap_delete *xlrec = (xl_heap_delete *) record->main_data;

			if (fb_record_block_has_applicable_image(&record->blocks[block_index]))
			{
				fb_replay_future_block_record_set_materializes(before);
				return true;
			}

			fb_replay_future_block_record_add_old(before, xlrec->offnum);
			return true;
		}
		case FB_WAL_RECORD_HEAP_LOCK:
		{
			xl_heap_lock *xlrec = (xl_heap_lock *) record->main_data;

			if (fb_record_block_has_applicable_image(&record->blocks[block_index]))
			{
				fb_replay_future_block_record_set_materializes(before);
				return true;
			}

			fb_replay_future_block_record_add_old(before, xlrec->offnum);
			return true;
		}
		case FB_WAL_RECORD_HEAP_CONFIRM:
		{
			xl_heap_confirm *xlrec = (xl_heap_confirm *) record->main_data;

			if (fb_record_block_has_applicable_image(&record->blocks[block_index]))
			{
				fb_replay_future_block_record_set_materializes(before);
				return true;
			}

			fb_replay_future_block_record_add_old(before, xlrec->offnum);
			return true;
		}
		case FB_WAL_RECORD_HEAP_INPLACE:
		{
			xl_heap_inplace *xlrec = (xl_heap_inplace *) record->main_data;

			if (fb_record_block_has_applicable_image(&record->blocks[block_index]))
			{
				fb_replay_future_block_record_set_materializes(before);
				return true;
			}

			fb_replay_future_block_record_add_old(before, xlrec->offnum);
			return true;
		}
		case FB_WAL_RECORD_HEAP2_LOCK_UPDATED:
		{
			xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *) record->main_data;

			if (fb_record_block_has_applicable_image(&record->blocks[block_index]))
			{
				fb_replay_future_block_record_set_materializes(before);
				return true;
			}

			fb_replay_future_block_record_add_old(before, xlrec->offnum);
			return true;
		}
		case FB_WAL_RECORD_HEAP2_PRUNE:
		{
#if PG_VERSION_NUM >= 170000
			xl_heap_prune xlrec;

			if (record->main_data == NULL ||
				record->main_data_len < SizeOfHeapPrune ||
				!record->blocks[block_index].has_data ||
				record->blocks[block_index].data_len == 0)
				return true;

			memcpy(&xlrec, record->main_data, SizeOfHeapPrune);
			if ((xlrec.flags & XLHP_HAS_NOW_UNUSED_ITEMS) != 0)
			{
				OffsetNumber redirected_buf[MaxHeapTuplesPerPage * 2];
				OffsetNumber nowdead_buf[MaxHeapTuplesPerPage];
				OffsetNumber nowunused_buf[MaxHeapTuplesPerPage];
				OffsetNumber *redirected = redirected_buf;
				OffsetNumber *nowdead = nowdead_buf;
				OffsetNumber *nowunused = nowunused_buf;
				OffsetNumber *frz_offsets = NULL;
				xlhp_freeze_plan *plans = NULL;
				int nredirected = 0;
				int ndead = 0;
				int nunused = 0;
				int nplans = 0;
				int i;

				heap_xlog_deserialize_prune_and_freeze(
					record->blocks[block_index].data,
					xlrec.flags,
					&nplans,
					&plans,
					&frz_offsets,
					&nredirected,
					&redirected,
					&ndead,
					&nowdead,
					&nunused,
					&nowunused);

				for (i = 0; i < nunused; i++)
					(void) fb_replay_future_block_record_remove_new(before,
														  nowunused[i]);
			}
#endif
			return true;
		}
		case FB_WAL_RECORD_XLOG_FPI:
		case FB_WAL_RECORD_XLOG_FPI_FOR_HINT:
			if (fb_wal_record_block_materializes_page(record, block_index))
				fb_replay_future_block_record_set_materializes(before);
			else
				before->valid = true;
			return true;
		default:
			return false;
	}
}

static bool
fb_replay_page_supports_future_block_record(Page page,
											const FbReplayFutureBlockRecord *future)
{
	int member;

	if (future == NULL || !future->valid || future->materializes_page)
		return true;

	member = -1;
	while ((member = bms_next_member(future->old_tuple_offsets, member)) >= 0)
	{
		if (fb_replay_get_old_tuple_from_page(page, (OffsetNumber) member, NULL) == NULL)
			return false;
	}

	member = -1;
	while ((member = bms_next_member(future->new_tuple_slot_offsets, member)) >= 0)
	{
		if (!fb_replay_page_has_insert_room(page, (OffsetNumber) member))
			return false;
	}

	return true;
}

static bool
fb_replay_page_supports_future_same_block_record(const FbRecordRef *record,
												 const FbReplayBlockKey *key,
												 Page page,
												 bool *relevant)
{
	if (relevant != NULL)
		*relevant = false;
	if (record == NULL || key == NULL)
		return true;

	switch (record->kind)
	{
		case FB_WAL_RECORD_HEAP_INSERT:
		{
			xl_heap_insert *xlrec = (xl_heap_insert *) record->main_data;
			int match_index = -1;

			for (int block_index = 0; block_index < record->block_count; block_index++)
			{
				if (!fb_replay_block_ref_matches_key(&record->blocks[block_index], key))
					continue;
				match_index = block_index;
				break;
			}
			if (match_index < 0)
				return true;
			if (relevant != NULL)
				*relevant = true;
			if (fb_wal_record_block_materializes_page(record, match_index))
				return true;
			return fb_replay_page_has_insert_room(page, xlrec->offnum);
		}
		case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
		{
			xl_heap_multi_insert *xlrec =
				(xl_heap_multi_insert *) record->main_data;
			int match_index = -1;

			for (int block_index = 0; block_index < record->block_count; block_index++)
			{
				if (!fb_replay_block_ref_matches_key(&record->blocks[block_index], key))
					continue;
				match_index = block_index;
				break;
			}
			if (match_index < 0)
				return true;
			if (relevant != NULL)
				*relevant = true;
			if (fb_wal_record_block_materializes_page(record, match_index))
				return true;
			for (int tuple_index = 0; tuple_index < xlrec->ntuples; tuple_index++)
			{
				OffsetNumber offnum;

				if (record->init_page)
					offnum = FirstOffsetNumber + tuple_index;
				else
					offnum = xlrec->offsets[tuple_index];
				if (!fb_replay_page_has_insert_room(page, offnum))
					return false;
			}
			return true;
		}
		case FB_WAL_RECORD_HEAP_UPDATE:
		case FB_WAL_RECORD_HEAP_HOT_UPDATE:
		{
			xl_heap_update *xlrec = (xl_heap_update *) record->main_data;
			bool matched = false;
			bool supported = true;
			int block_index;

			for (block_index = 0; block_index < record->block_count; block_index++)
			{
				if (!fb_replay_block_ref_matches_key(&record->blocks[block_index], key))
					continue;

				matched = true;
				if (fb_replay_record_block_has_wal_id(record, block_index, 1))
				{
					supported = fb_replay_get_old_tuple_from_page(page,
															  xlrec->old_offnum,
															  NULL) != NULL;
				}
				else if (fb_replay_record_block_has_wal_id(record, block_index, 0))
				{
					if (!fb_wal_record_block_materializes_page(record, block_index))
						supported = fb_replay_page_has_insert_room(page,
														   xlrec->new_offnum);
				}
				else
				{
					supported = (fb_replay_get_old_tuple_from_page(page,
															   xlrec->old_offnum,
															   NULL) != NULL) &&
						fb_replay_page_has_insert_room(page, xlrec->new_offnum);
				}

				if (!supported)
					break;
			}

			if (matched && relevant != NULL)
				*relevant = true;
			return supported;
		}
		case FB_WAL_RECORD_HEAP_DELETE:
		{
			xl_heap_delete *xlrec = (xl_heap_delete *) record->main_data;
			int match_index = -1;

			for (int block_index = 0; block_index < record->block_count; block_index++)
			{
				if (!fb_replay_block_ref_matches_key(&record->blocks[block_index], key))
					continue;
				match_index = block_index;
				break;
			}
			if (match_index < 0)
				return true;
			if (relevant != NULL)
				*relevant = true;
			return fb_replay_get_old_tuple_from_page(page, xlrec->offnum, NULL) != NULL;
		}
		case FB_WAL_RECORD_HEAP_LOCK:
		{
			xl_heap_lock *xlrec = (xl_heap_lock *) record->main_data;
			int match_index = -1;

			for (int block_index = 0; block_index < record->block_count; block_index++)
			{
				if (!fb_replay_block_ref_matches_key(&record->blocks[block_index], key))
					continue;
				match_index = block_index;
				break;
			}
			if (match_index < 0)
				return true;
			if (relevant != NULL)
				*relevant = true;
			return fb_replay_get_old_tuple_from_page(page, xlrec->offnum, NULL) != NULL;
		}
		case FB_WAL_RECORD_HEAP_CONFIRM:
		{
			xl_heap_confirm *xlrec = (xl_heap_confirm *) record->main_data;
			int match_index = -1;

			for (int block_index = 0; block_index < record->block_count; block_index++)
			{
				if (!fb_replay_block_ref_matches_key(&record->blocks[block_index], key))
					continue;
				match_index = block_index;
				break;
			}
			if (match_index < 0)
				return true;
			if (relevant != NULL)
				*relevant = true;
			return fb_replay_get_old_tuple_from_page(page, xlrec->offnum, NULL) != NULL;
		}
		case FB_WAL_RECORD_HEAP_INPLACE:
		{
			xl_heap_inplace *xlrec = (xl_heap_inplace *) record->main_data;
			int match_index = -1;

			for (int block_index = 0; block_index < record->block_count; block_index++)
			{
				if (!fb_replay_block_ref_matches_key(&record->blocks[block_index], key))
					continue;
				match_index = block_index;
				break;
			}
			if (match_index < 0)
				return true;
			if (relevant != NULL)
				*relevant = true;
			return fb_replay_get_old_tuple_from_page(page, xlrec->offnum, NULL) != NULL;
		}
		case FB_WAL_RECORD_HEAP2_LOCK_UPDATED:
		{
			xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *) record->main_data;
			int match_index = -1;

			for (int block_index = 0; block_index < record->block_count; block_index++)
			{
				if (!fb_replay_block_ref_matches_key(&record->blocks[block_index], key))
					continue;
				match_index = block_index;
				break;
			}
			if (match_index < 0)
				return true;
			if (relevant != NULL)
				*relevant = true;
			return fb_replay_get_old_tuple_from_page(page, xlrec->offnum, NULL) != NULL;
		}
		case FB_WAL_RECORD_XLOG_FPI:
		case FB_WAL_RECORD_XLOG_FPI_FOR_HINT:
		{
			int block_index;

			for (block_index = 0; block_index < record->block_count; block_index++)
			{
				if (!fb_replay_block_ref_matches_key(&record->blocks[block_index],
													 key))
					continue;
				if (relevant != NULL)
					*relevant = true;
				return fb_wal_record_block_materializes_page(record, block_index);
			}
			return true;
		}
		default:
			return true;
	}
}

static bool
fb_replay_prune_image_should_preserve_for_next_record(const FbReplayBlockKey *key,
													  Page current_page,
													  Page image_page,
													  const FbRecordRef *next_record)
{
	bool relevant = false;
	bool current_ok;
	bool image_ok;

	current_ok = fb_replay_page_supports_future_same_block_record(next_record,
															  key,
															  current_page,
															  &relevant);
	if (!relevant)
		return false;

	image_ok = fb_replay_page_supports_future_same_block_record(next_record,
															key,
															image_page,
															NULL);
	return current_ok && !image_ok;
}

static bool
fb_replay_build_prune_lookahead(const FbWalRecordIndex *index,
								HTAB **lookahead_out)
{
	FbWalRecordCursor *cursor;
	HTAB *future_by_block;
	HTAB *lookahead;
	FbRecordRef record;
	uint32 record_index;

	if (lookahead_out != NULL)
		*lookahead_out = NULL;
	if (index == NULL || index->record_count == 0 || lookahead_out == NULL)
		return false;

	future_by_block = fb_replay_create_future_block_hash();
	lookahead = fb_replay_create_prune_lookahead_hash();
	cursor = fb_wal_record_cursor_open(index, FB_SPOOL_BACKWARD);
	while (fb_wal_record_cursor_read(cursor, &record, &record_index))
	{
		if (record.kind == FB_WAL_RECORD_HEAP2_PRUNE &&
			record.block_count > 0)
		{
			FbReplayBlockKey key = fb_replay_block_key_from_ref(&record.blocks[0]);
			FbReplayFutureBlockEntry *future_entry;

			future_entry = (FbReplayFutureBlockEntry *) hash_search(future_by_block,
																&key,
																HASH_FIND,
																NULL);
			if (future_entry != NULL && future_entry->future.valid)
			{
				FbReplayPruneLookaheadEntry *entry;
				bool found;

				entry = (FbReplayPruneLookaheadEntry *) hash_search(lookahead,
																 &record_index,
																 HASH_ENTER,
																 &found);
				if (!found)
					entry->record_index = record_index;
				fb_replay_future_block_record_clone(&entry->future,
													 &future_entry->future);
			}
		}

		for (int block_index = 0; block_index < record.block_count; block_index++)
		{
			FbReplayFutureBlockEntry *future_entry;
			FbReplayBlockKey key;
			bool found;

			if (!record.blocks[block_index].in_use)
				continue;

			key = fb_replay_block_key_from_ref(&record.blocks[block_index]);
			future_entry = (FbReplayFutureBlockEntry *) hash_search(future_by_block,
																&key,
																HASH_ENTER,
																&found);
			if (!found)
				future_entry->key = key;
			if (!fb_replay_future_block_record_compose(&record,
												   block_index,
												   found ? &future_entry->future : NULL,
												   &future_entry->future))
				continue;
		}
	}
	fb_wal_record_cursor_close(cursor);
	hash_destroy(future_by_block);
	*lookahead_out = lookahead;
	return true;
}

static bool
fb_replay_prune_image_should_preserve_page(const FbRecordRef *record,
										   const FbReplayBlockState *state,
										   const FbReplayControl *control)
{
	const FbRecordBlockRef *block_ref;
	FbReplayPruneLookaheadEntry *entry;
	bool current_ok;
	bool image_ok;

	if (record == NULL || state == NULL || !state->initialized ||
		control == NULL || control->phase != FB_REPLAY_PHASE_FINAL ||
		control->prune_lookahead == NULL)
		return false;

	/*
	 * Final replay reuses the warm store. If the current page state already
	 * comes from a later WAL record, preserving it across an older prune image
	 * would collapse history and skip the image-derived free space/base state.
	 */
	if (!XLogRecPtrIsInvalid(state->page_lsn) &&
		state->page_lsn > record->end_lsn)
		return false;

	block_ref = &record->blocks[0];
	if (!fb_record_block_has_applicable_image(block_ref) ||
		block_ref->image == NULL ||
		(block_ref->has_data && block_ref->data_len > 0))
		return false;

	entry = (FbReplayPruneLookaheadEntry *) hash_search(control->prune_lookahead,
														&control->record_index,
														HASH_FIND,
														NULL);
	if (entry == NULL)
		return false;

	current_ok = fb_replay_page_supports_future_block_record((Page) state->page,
															 &entry->future);
	image_ok = fb_replay_page_supports_future_block_record((Page) block_ref->image,
														   &entry->future);
	return current_ok && !image_ok;
}

static void
fb_replay_prune_remove_offset(OffsetNumber *offsets,
							  int *count,
							  OffsetNumber offnum)
{
	int i;

	if (offsets == NULL || count == NULL || *count <= 0)
		return;

	for (i = 0; i < *count; i++)
	{
		if (offsets[i] != offnum)
			continue;
		memmove(&offsets[i], &offsets[i + 1],
				sizeof(OffsetNumber) * (*count - i - 1));
		(*count)--;
		return;
	}
}

static bool
fb_replay_prune_has_offset(const OffsetNumber *offsets,
						   int count,
						   OffsetNumber offnum)
{
	int i;

	if (offsets == NULL || count <= 0)
		return false;

	for (i = 0; i < count; i++)
	{
		if (offsets[i] == offnum)
			return true;
	}

	return false;
}

static void
fb_replay_prune_add_nowunused(OffsetNumber *nowunused,
							  int *nunused,
							  OffsetNumber offnum)
{
	if (nowunused == NULL || nunused == NULL || offnum < FirstOffsetNumber)
		return;
	if (fb_replay_prune_has_offset(nowunused, *nunused, offnum))
		return;

	nowunused[*nunused] = offnum;
	(*nunused)++;
}

static bool
fb_replay_prune_remove_redirect(OffsetNumber *redirected,
								int *nredirected,
								OffsetNumber offnum)
{
	int i;

	if (redirected == NULL || nredirected == NULL || *nredirected <= 0)
		return false;

	for (i = 0; i < (*nredirected * 2); i += 2)
	{
		if (redirected[i] != offnum)
			continue;
		memmove(&redirected[i], &redirected[i + 2],
				sizeof(OffsetNumber) * (*nredirected * 2 - i - 2));
		(*nredirected)--;
		return true;
	}

	return false;
}

static void
fb_replay_prune_apply_future_guard(const FbReplayControl *control,
								   OffsetNumber *redirected,
								   int *nredirected,
								   OffsetNumber *nowdead,
								   int *ndead,
								   OffsetNumber *nowunused,
								   int *nunused)
{
	FbReplayPruneLookaheadEntry *entry;
	int i;

	if (control == NULL || control->phase != FB_REPLAY_PHASE_FINAL ||
		control->prune_lookahead == NULL)
		return;

	entry = (FbReplayPruneLookaheadEntry *) hash_search(control->prune_lookahead,
														&control->record_index,
														HASH_FIND,
														NULL);
	if (entry == NULL || !entry->future.valid || entry->future.materializes_page)
		return;

	i = -1;
	while ((i = bms_next_member(entry->future.old_tuple_offsets, i)) >= 0)
	{
		OffsetNumber offnum = (OffsetNumber) i;

		/*
		 * Future old-tuple lookups already accept LP_DEAD and redirected line
		 * pointers. Only suppress transitions that would discard the tuple
		 * payload entirely.
		 */
		fb_replay_prune_remove_offset(nowunused, nunused, offnum);
	}

	i = -1;
	while ((i = bms_next_member(entry->future.new_tuple_slot_offsets, i)) >= 0)
	{
		OffsetNumber offnum = (OffsetNumber) i;
		bool removed_dead;
		bool removed_redirect;

		removed_dead = fb_replay_prune_has_offset(nowdead, *ndead, offnum);
		if (removed_dead)
			fb_replay_prune_remove_offset(nowdead, ndead, offnum);
		removed_redirect = fb_replay_prune_remove_redirect(redirected,
														 nredirected,
														 offnum);
		if (!removed_dead && !removed_redirect &&
			!fb_replay_prune_has_offset(nowunused, *nunused, offnum))
			continue;
		fb_replay_prune_add_nowunused(nowunused, nunused, offnum);
	}
}

/*
 * fb_replay_heap_insert
 *    Replay helper.
 */

static void
fb_replay_heap_insert(const FbRelationInfo *info,
					  const FbRecordRef *record,
					  FbReplayStore *store,
					  TupleDesc tupdesc,
					  TupleDesc toast_tupdesc,
					  FbToastStore *toast_store,
					  FbReplayControl *control,
					  FbReplayResult *result,
					  FbReverseOpSource *source,
					  FbPendingReverseOpQueue *pending_ops)
{
	xl_heap_insert *xlrec = (xl_heap_insert *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	union
	{
		HeapTupleHeaderData hdr;
		char data[MaxHeapTupleSize];
	} tbuf;
	HeapTupleHeader htup;
	xl_heap_header xlhdr;
	char *data;
	Size datalen;
	uint32 newlen;
	ItemPointerData target_tid;
	bool image_applied;
	bool ready;
	const char *phase = "none";

	if (control != NULL)
	{
		switch (control->phase)
		{
			case FB_REPLAY_PHASE_DISCOVER:
				phase = "discover";
				break;
			case FB_REPLAY_PHASE_WARM:
				phase = "warm";
				break;
			case FB_REPLAY_PHASE_FINAL:
				phase = "final";
				break;
		}
	}

	fb_replay_ensure_block_ready(store, block_ref, record, record->init_page,
								 control, result, &state, &ready);
	if (!ready)
		return;
	page = (Page) state->page;
	image_applied = fb_record_block_has_applicable_image(block_ref);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395737 &&
		block_ref->blkno == 38724)
		fb_replay_debug_log_documents38724_page("insert before", record->lsn, page);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 77425251 &&
		block_ref->blkno == 488294)
		elog(WARNING,
			 "fb_debug blk488294 insert before lsn=%X/%08X off=%u maxoff=%u lp1=%s lp2=%s lp3=%s lp4=%s",
			 LSN_FORMAT_ARGS(record->lsn),
			 xlrec->offnum,
			 (unsigned int) PageGetMaxOffsetNumber(page),
			 fb_replay_debug_lp_state(page, 1),
			 fb_replay_debug_lp_state(page, 2),
			 fb_replay_debug_lp_state(page, 3),
			 fb_replay_debug_lp_state(page, 4));
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
		block_ref->blkno == 17079)
		fb_replay_debug_log_toast17079_page("insert before", record->lsn, page);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
		block_ref->blkno == 26273)
		fb_replay_debug_log_toast26273_page("insert before", record->lsn, page);
	ItemPointerSetBlockNumber(&target_tid, block_ref->blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	if (!image_applied)
	{
		if (!block_ref->has_data || block_ref->data_len <= SizeOfHeapHeader)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("heap insert record missing block data")));

		data = block_ref->data;
		datalen = block_ref->data_len;
		newlen = datalen - SizeOfHeapHeader;
		memcpy(&xlhdr, data, SizeOfHeapHeader);
		data += SizeOfHeapHeader;

		htup = &tbuf.hdr;
		MemSet(htup, 0, SizeofHeapTupleHeader);
		memcpy((char *) htup + SizeofHeapTupleHeader, data, newlen);
		newlen += SizeofHeapTupleHeader;
		htup->t_infomask2 = xlhdr.t_infomask2;
		htup->t_infomask = xlhdr.t_infomask;
		htup->t_hoff = xlhdr.t_hoff;
		HeapTupleHeaderSetXmin(htup, record->xid);
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		htup->t_ctid = target_tid;
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
			block_ref->blkno == 17079)
			elog(WARNING,
				 "fb_debug toast insert tuple blk=17079 lsn=%X/%08X off=%u datalen=%u newlen=%u image_applied=%s has_data=%s",
				 LSN_FORMAT_ARGS(record->lsn),
				 xlrec->offnum,
				 (unsigned int) datalen,
				 (unsigned int) newlen,
				 image_applied ? "true" : "false",
				 block_ref->has_data ? "true" : "false");
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
			block_ref->blkno == 26273)
			elog(WARNING,
				 "fb_debug toast insert tuple blk=26273 lsn=%X/%08X off=%u datalen=%u newlen=%u image_applied=%s has_data=%s",
				 LSN_FORMAT_ARGS(record->lsn),
				 xlrec->offnum,
				 (unsigned int) datalen,
				 (unsigned int) newlen,
				 image_applied ? "true" : "false",
				 block_ref->has_data ? "true" : "false");

		if (!fb_replay_expand_unused_slots(page, xlrec->offnum))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("failed to prepare heap insert line pointers"),
					 errdetail("lsn=%X/%08X rel=%u/%u/%u blk=%u off=%u maxoff=%u",
							   LSN_FORMAT_ARGS(record->lsn),
							   FB_LOCATOR_SPCOID(block_ref->locator),
							   FB_LOCATOR_DBOID(block_ref->locator),
							   FB_LOCATOR_RELNUMBER(block_ref->locator),
							   block_ref->blkno,
							   xlrec->offnum,
							   (unsigned int) PageGetMaxOffsetNumber(page))));
		if (PageAddItem(page, (Item) htup, newlen, xlrec->offnum, true, true) == InvalidOffsetNumber)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("failed to replay heap insert"),
					 errdetail("phase=%s recidx=%u lsn=%X/%08X rel=%u/%u/%u blk=%u off=%u image=%s apply_image=%s data=%s init_page=%s maxoff=%u has_item=%s lower=%u upper=%u exact_free=%u heap_free=%u page_lsn=%X/%08X lp23=%s lp24=%s lp41=%s",
							   phase,
							   control != NULL ? control->record_index : 0,
							   LSN_FORMAT_ARGS(record->lsn),
							   FB_LOCATOR_SPCOID(block_ref->locator),
							   FB_LOCATOR_DBOID(block_ref->locator),
							   FB_LOCATOR_RELNUMBER(block_ref->locator),
							   block_ref->blkno,
							   xlrec->offnum,
							   block_ref->has_image ? "true" : "false",
							   block_ref->apply_image ? "true" : "false",
							   block_ref->has_data ? "true" : "false",
							   record->init_page ? "true" : "false",
							   (unsigned int) PageGetMaxOffsetNumber(page),
							   (PageGetMaxOffsetNumber(page) >= xlrec->offnum &&
								ItemIdIsUsed(PageGetItemId(page, xlrec->offnum))) ? "true" : "false",
							   (unsigned int) ((PageHeader) page)->pd_lower,
							   (unsigned int) ((PageHeader) page)->pd_upper,
							   (unsigned int) PageGetExactFreeSpace(page),
							   (unsigned int) PageGetHeapFreeSpace(page),
							   LSN_FORMAT_ARGS(state->page_lsn),
							   fb_replay_debug_lp_state(page, 23),
							   fb_replay_debug_lp_state(page, 24),
							   fb_replay_debug_lp_state(page, 41))));
	}
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
		block_ref->blkno == 17079)
		fb_replay_debug_log_toast17079_page("insert after", record->end_lsn, page);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
		block_ref->blkno == 26273)
		fb_replay_debug_log_toast26273_page("insert after", record->end_lsn, page);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395737 &&
		block_ref->blkno == 38724)
		fb_replay_debug_log_documents38724_page("insert after", record->end_lsn, page);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 77425251 &&
		block_ref->blkno == 488294)
		elog(WARNING,
			 "fb_debug blk488294 insert after lsn=%X/%08X off=%u maxoff=%u lp1=%s lp2=%s lp3=%s lp4=%s",
			 LSN_FORMAT_ARGS(record->end_lsn),
			 xlrec->offnum,
			 (unsigned int) PageGetMaxOffsetNumber(page),
			 fb_replay_debug_lp_state(page, 1),
			 fb_replay_debug_lp_state(page, 2),
			 fb_replay_debug_lp_state(page, 3),
			 fb_replay_debug_lp_state(page, 4));
	state->page_lsn = record->end_lsn;
	result->records_replayed++;
	if (record->committed_after_target && block_ref->is_main_relation &&
		!fb_record_is_speculative_insert(record))
		result->target_insert_count++;

	if (block_ref->is_toast_relation && toast_store != NULL && toast_tupdesc != NULL)
	{
		if (image_applied)
			fb_toast_store_sync_page(toast_store, toast_tupdesc, page, block_ref->blkno);
		else
		{
			HeapTuple toast_tuple = fb_copy_page_tuple(page, block_ref->blkno, xlrec->offnum);

			fb_toast_store_put_from_tuple(toast_store, toast_tupdesc, toast_tuple);
		}
	}

	if (source != NULL && tupdesc != NULL &&
		record->committed_after_target && block_ref->is_main_relation &&
		!fb_record_is_speculative_insert(record))
	{
		HeapTuple new_tuple = fb_copy_page_tuple(page, block_ref->blkno, xlrec->offnum);

		if (new_tuple != NULL)
			fb_append_forward_insert(info, tupdesc, toast_store, record, new_tuple,
									result, source, pending_ops);
	}
}

/*
 * fb_replay_heap_delete
 *    Replay helper.
 */

static void
fb_replay_heap_delete(const FbRelationInfo *info,
					  const FbRecordRef *record,
					  FbReplayStore *store,
					  TupleDesc tupdesc,
					  TupleDesc toast_tupdesc,
					  FbToastStore *toast_store,
					  FbReplayControl *control,
					  FbReplayResult *result,
					  FbReverseOpSource *source,
					  FbPendingReverseOpQueue *pending_ops)
{
	xl_heap_delete *xlrec = (xl_heap_delete *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	HeapTupleHeader htup;
	ItemPointerData target_tid;
	HeapTuple old_tuple = NULL;
	HeapTuple old_toast_tuple = NULL;
	bool needs_redo;
	bool ready;

	fb_replay_ensure_block_ready(store, block_ref, record, false,
								 control, result, &state, &ready);
	if (!ready)
		return;
	page = (Page) state->page;
	if ((FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
		 block_ref->blkno == 26273) ||
		record->lsn == ((uint64) 0x87 << 32 | 0x6E0B1F10))
		elog(WARNING,
			 "fb_debug toast delete enter rel=%u blk=%u lsn=%X/%08X off=%u page_lsn=%X/%08X maxoff=%u lp27=%s lp28=%s",
			 FB_LOCATOR_RELNUMBER(block_ref->locator),
			 block_ref->blkno,
			 LSN_FORMAT_ARGS(record->lsn),
			 xlrec->offnum,
			 LSN_FORMAT_ARGS(state->page_lsn),
			 (unsigned int) PageGetMaxOffsetNumber(page),
			 fb_replay_debug_lp_state(page, 27),
			 fb_replay_debug_lp_state(page, 28));
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395737 &&
		block_ref->blkno == 38724)
		elog(WARNING,
			 "fb_debug documents delete enter rel=%u blk=%u lsn=%X/%08X off=%u page_lsn=%X/%08X maxoff=%u lp1=%s lp2=%s lp3=%s lp4=%s",
			 FB_LOCATOR_RELNUMBER(block_ref->locator),
			 block_ref->blkno,
			 LSN_FORMAT_ARGS(record->lsn),
			 xlrec->offnum,
			 LSN_FORMAT_ARGS(state->page_lsn),
			 (unsigned int) PageGetMaxOffsetNumber(page),
			 fb_replay_debug_lp_state(page, 1),
			 fb_replay_debug_lp_state(page, 2),
			 fb_replay_debug_lp_state(page, 3),
			 fb_replay_debug_lp_state(page, 4));
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 77425251 &&
		block_ref->blkno == 488294)
		elog(WARNING,
			 "fb_debug blk488294 delete before lsn=%X/%08X off=%u maxoff=%u lp1=%s lp2=%s lp3=%s lp4=%s",
			 LSN_FORMAT_ARGS(record->lsn),
			 xlrec->offnum,
			 (unsigned int) PageGetMaxOffsetNumber(page),
			 fb_replay_debug_lp_state(page, 1),
			 fb_replay_debug_lp_state(page, 2),
			 fb_replay_debug_lp_state(page, 3),
			 fb_replay_debug_lp_state(page, 4));
	fb_replay_sync_toast_page_if_image(block_ref, page, toast_tupdesc, toast_store);
	needs_redo = fb_replay_block_needs_redo(block_ref);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
		block_ref->blkno == 17079)
		fb_replay_debug_log_toast17079_page("delete before", record->lsn, page);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
		block_ref->blkno == 26273)
		fb_replay_debug_log_toast26273_page("delete before", record->lsn, page);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395737 &&
		block_ref->blkno == 38724)
		fb_replay_debug_log_documents38724_page("delete before", record->lsn, page);
	if (source != NULL && tupdesc != NULL &&
		record->committed_after_target && block_ref->is_main_relation)
		old_tuple = fb_copy_page_old_tuple(page, block_ref->blkno, xlrec->offnum);
	if (block_ref->is_toast_relation && toast_store != NULL && toast_tupdesc != NULL)
		old_toast_tuple = fb_copy_page_old_tuple(page, block_ref->blkno, xlrec->offnum);

	if (needs_redo)
	{
		htup = fb_replay_get_old_tuple_from_page(page, xlrec->offnum, NULL);
		if (htup == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("failed to locate tuple for heap delete redo"),
					 errdetail("lsn=%X/%08X rel=%u/%u/%u blk=%u off=%u image=%s apply_image=%s data=%s init_page=%s maxoff=%u has_item=%s page_lsn=%X/%08X lp27=%s lp28=%s",
							   LSN_FORMAT_ARGS(record->lsn),
							   FB_LOCATOR_SPCOID(block_ref->locator),
							   FB_LOCATOR_DBOID(block_ref->locator),
							   FB_LOCATOR_RELNUMBER(block_ref->locator),
							   block_ref->blkno,
							   xlrec->offnum,
							   block_ref->has_image ? "true" : "false",
							   block_ref->apply_image ? "true" : "false",
							   block_ref->has_data ? "true" : "false",
							   record->init_page ? "true" : "false",
							   (unsigned int) PageGetMaxOffsetNumber(page),
							   (PageGetMaxOffsetNumber(page) >= xlrec->offnum &&
								ItemIdIsUsed(PageGetItemId(page, xlrec->offnum))) ? "true" : "false",
							   LSN_FORMAT_ARGS(state->page_lsn),
							   fb_replay_debug_lp_state(page, 27),
							   fb_replay_debug_lp_state(page, 28))));

		ItemPointerSetBlockNumber(&target_tid, block_ref->blkno);
		ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

		htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		HeapTupleHeaderClearHotUpdated(htup);
		fb_fix_infomask_from_infobits(xlrec->infobits_set,
									  &htup->t_infomask, &htup->t_infomask2);
		if ((xlrec->flags & XLH_DELETE_IS_SUPER) == 0)
			HeapTupleHeaderSetXmax(htup, xlrec->xmax);
		else
			HeapTupleHeaderSetXmin(htup, InvalidTransactionId);
		HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

		PageSetPrunable(page, record->xid);
		if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(page);
		if (xlrec->flags & XLH_DELETE_IS_PARTITION_MOVE)
			HeapTupleHeaderSetMovedPartitions(htup);
		else
			htup->t_ctid = target_tid;
	}

	state->page_lsn = record->end_lsn;
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
		block_ref->blkno == 17079)
		fb_replay_debug_log_toast17079_page("delete after", record->end_lsn, page);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
		block_ref->blkno == 26273)
		fb_replay_debug_log_toast26273_page("delete after", record->end_lsn, page);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395737 &&
		block_ref->blkno == 38724)
		fb_replay_debug_log_documents38724_page("delete after", record->end_lsn, page);
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 77425251 &&
		block_ref->blkno == 488294)
		elog(WARNING,
			 "fb_debug blk488294 delete after lsn=%X/%08X off=%u maxoff=%u lp1=%s lp2=%s lp3=%s lp4=%s",
			 LSN_FORMAT_ARGS(record->end_lsn),
			 xlrec->offnum,
			 (unsigned int) PageGetMaxOffsetNumber(page),
			 fb_replay_debug_lp_state(page, 1),
			 fb_replay_debug_lp_state(page, 2),
			 fb_replay_debug_lp_state(page, 3),
			 fb_replay_debug_lp_state(page, 4));
	result->records_replayed++;
	if (record->committed_after_target && block_ref->is_main_relation &&
		!fb_record_is_super_delete(record))
		result->target_delete_count++;

	if (old_tuple != NULL && !fb_record_is_super_delete(record))
		fb_append_forward_delete(info, tupdesc, toast_store, record, old_tuple,
								 result, source, pending_ops);
	else if (old_tuple != NULL)
		heap_freetuple(old_tuple);
	if (old_toast_tuple != NULL)
		fb_toast_store_remove_from_tuple(toast_store, toast_tupdesc, old_toast_tuple);
}

/*
 * fb_replay_heap_update
 *    Replay helper.
 */

static void
fb_replay_heap_update(const FbRelationInfo *info,
					  const FbRecordRef *record,
					  FbReplayStore *store,
					  bool hot_update,
					  TupleDesc tupdesc,
					  TupleDesc toast_tupdesc,
					  FbToastStore *toast_store,
					  FbReplayControl *control,
					  FbReplayResult *result,
					  FbReverseOpSource *source,
					  FbPendingReverseOpQueue *pending_ops)
{
	xl_heap_update *xlrec = (xl_heap_update *) record->main_data;
	FbRecordBlockRef *new_block_ref =
		fb_replay_find_block_ref_by_wal_id((FbRecordRef *) record, 0);
	FbRecordBlockRef *old_block_ref =
		fb_replay_find_block_ref_by_wal_id((FbRecordRef *) record, 1);
	FbReplayBlockState *old_state;
	FbReplayBlockState *new_state;
	FbRecordBlockRef *tuple_data_ref;
	bool new_page_image_applied;
	Page oldpage;
	Page newpage;
	HeapTupleHeader oldtup;
	ItemPointerData newtid;
	uint32 oldtup_len = 0;
	ItemId lp;
	OffsetNumber offnum;
	uint16 prefixlen = 0;
	uint16 suffixlen = 0;
	char *recdata;
	char *recdata_end;
	Size datalen;
	Size tuplen;
	xl_heap_header xlhdr;
	char *newp;
	uint32 newlen;
	HeapTupleHeader htup;
	HeapTuple old_tuple = NULL;
	HeapTuple new_tuple = NULL;
	HeapTuple old_toast_tuple = NULL;
	HeapTuple new_toast_tuple = NULL;
	union
	{
		HeapTupleHeaderData hdr;
		char data[MaxHeapTupleSize];
	} tbuf;
	bool old_ready;
	bool new_ready = false;
	bool old_needs_redo;
	bool need_old_tuple_data;
	bool old_payload_needed;
	int old_block_index;
	int new_block_index;

	if (new_block_ref == NULL && record->block_count > 0)
		new_block_ref = (FbRecordBlockRef *) &record->blocks[0];
	if (old_block_ref == NULL)
		old_block_ref = (record->block_count > 1) ?
			(FbRecordBlockRef *) &record->blocks[1] : new_block_ref;
	if (new_block_ref == NULL || old_block_ref == NULL)
		elog(ERROR, "heap update record missing block references");
	old_block_index = (old_block_ref->block_id == 1) ? 1 :
		(old_block_ref == new_block_ref ? 0 : 1);
	new_block_index = 0;

	fb_replay_ensure_block_ready(store,
								 old_block_ref,
								 record,
								 fb_record_allows_init_for_block(record, old_block_index),
								 control, result, &old_state, &old_ready);
	if (!old_ready)
	{
		if (control != NULL && control->phase == FB_REPLAY_PHASE_DISCOVER)
			fb_replay_note_record_missing_blocks(control, record);
		return;
	}
	if (old_block_ref == new_block_ref)
	{
		new_state = old_state;
		new_ready = true;
	}
	else
		fb_replay_ensure_block_ready(store,
									 new_block_ref,
									 record,
									 fb_record_allows_init_for_block(record, new_block_index),
									 control, result, &new_state, &new_ready);
	if (!new_ready)
	{
		if (control != NULL && control->phase == FB_REPLAY_PHASE_DISCOVER)
			fb_replay_note_record_missing_blocks(control, record);
		return;
	}

	oldpage = (Page) old_state->page;
	newpage = (Page) new_state->page;
	if (FB_LOCATOR_RELNUMBER(new_block_ref->locator) == 77425251 &&
		new_block_ref->blkno == 488294)
		elog(WARNING,
			 "fb_debug blk488294 update before lsn=%X/%08X oldblk=%u old_off=%u new_off=%u same=%s old_maxoff=%u new_maxoff=%u new_lp1=%s new_lp2=%s new_lp3=%s new_lp4=%s",
			 LSN_FORMAT_ARGS(record->lsn),
			 old_block_ref->blkno,
			 xlrec->old_offnum,
			 xlrec->new_offnum,
			 old_block_ref == new_block_ref ? "true" : "false",
			 (unsigned int) PageGetMaxOffsetNumber(oldpage),
			 (unsigned int) PageGetMaxOffsetNumber(newpage),
			 fb_replay_debug_lp_state(newpage, 1),
			 fb_replay_debug_lp_state(newpage, 2),
			 fb_replay_debug_lp_state(newpage, 3),
			 fb_replay_debug_lp_state(newpage, 4));
	old_needs_redo = fb_replay_block_needs_redo(old_block_ref);
	new_page_image_applied = fb_record_block_has_applicable_image(new_block_ref);
	fb_replay_sync_toast_page_if_image(old_block_ref, oldpage, toast_tupdesc, toast_store);
	if (new_block_ref != old_block_ref)
		fb_replay_sync_toast_page_if_image(new_block_ref, newpage, toast_tupdesc, toast_store);

	if (source != NULL && tupdesc != NULL &&
		record->committed_after_target && old_block_ref->is_main_relation)
		old_tuple = fb_copy_page_old_tuple(oldpage, old_block_ref->blkno, xlrec->old_offnum);
	if (old_block_ref->is_toast_relation && toast_store != NULL && toast_tupdesc != NULL)
		old_toast_tuple = fb_copy_page_old_tuple(oldpage, old_block_ref->blkno, xlrec->old_offnum);

	ItemPointerSet(&newtid, new_block_ref->blkno, xlrec->new_offnum);

	old_payload_needed = !new_page_image_applied &&
		((xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD) != 0 ||
		 (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD) != 0);
	need_old_tuple_data = old_needs_redo || old_payload_needed;
	if (need_old_tuple_data)
	{
		offnum = xlrec->old_offnum;
		oldtup = fb_replay_get_old_tuple_from_page(oldpage, offnum, &oldtup_len);
		if (oldtup == NULL)
		{
			if (old_payload_needed)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("failed to locate tuple for heap update redo"),
						 errdetail("lsn=%X/%08X newblk=%u oldblk=%u sameblk=%s old_off=%u new_off=%u old_maxoff=%u old_lp=%s old_page_lsn=%X/%08X image=%s apply_image=%s data=%s init_page=%s flags=0x%02X",
								   LSN_FORMAT_ARGS(record->lsn),
								   new_block_ref->blkno,
								   old_block_ref->blkno,
								   old_block_ref == new_block_ref ? "true" : "false",
								   xlrec->old_offnum,
								   xlrec->new_offnum,
								   (unsigned int) PageGetMaxOffsetNumber(oldpage),
								   fb_replay_debug_lp_state(oldpage, xlrec->old_offnum),
								   LSN_FORMAT_ARGS(old_state->page_lsn),
								   old_block_ref->has_image ? "true" : "false",
								   old_block_ref->apply_image ? "true" : "false",
								   old_block_ref->has_data ? "true" : "false",
								   record->init_page ? "true" : "false",
								   xlrec->flags)));
			old_needs_redo = false;
		}

		if (PageGetMaxOffsetNumber(oldpage) < offnum)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("invalid old offnum during heap update redo")));
	}

	if (old_needs_redo)
	{
		lp = PageGetItemId(oldpage, xlrec->old_offnum);
		htup = (HeapTupleHeader) PageGetItem(oldpage, lp);

		htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		if (hot_update)
			HeapTupleHeaderSetHotUpdated(htup);
		else
			HeapTupleHeaderClearHotUpdated(htup);
		fb_fix_infomask_from_infobits(xlrec->old_infobits_set,
									  &htup->t_infomask, &htup->t_infomask2);
		HeapTupleHeaderSetXmax(htup, xlrec->old_xmax);
		HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
		htup->t_ctid = newtid;

		PageSetPrunable(oldpage, record->xid);
		if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(oldpage);
	}

	if (!new_page_image_applied)
	{
		tuple_data_ref = new_block_ref;
		if ((!tuple_data_ref->has_data || tuple_data_ref->data_len == 0) &&
			old_block_ref != new_block_ref &&
			old_block_ref->has_data && old_block_ref->data_len > 0)
			tuple_data_ref = old_block_ref;

		if (!tuple_data_ref->has_data || tuple_data_ref->data_len == 0)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("heap update record missing new tuple data"),
					 errdetail("lsn=%X/%08X newblk=%u oldblk=%u sameblk=%s new_has_image=%s new_has_data=%s old_has_data=%s block_count=%d",
							   LSN_FORMAT_ARGS(record->lsn),
							   new_block_ref->blkno,
							   old_block_ref->blkno,
							   old_block_ref == new_block_ref ? "true" : "false",
							   new_block_ref->has_image ? "true" : "false",
							   new_block_ref->has_data ? "true" : "false",
							   old_block_ref->has_data ? "true" : "false",
							   record->block_count)));

		recdata = tuple_data_ref->data;
		datalen = tuple_data_ref->data_len;
		recdata_end = recdata + datalen;
		offnum = xlrec->new_offnum;

		if (!fb_replay_expand_unused_slots(newpage, offnum))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("invalid new offnum during heap update redo"),
					 errdetail("lsn=%X/%08X newblk=%u oldblk=%u sameblk=%s off=%u maxoff=%u image=%s apply_image=%s data=%s init_page=%s flags=0x%02X",
							   LSN_FORMAT_ARGS(record->lsn),
							   new_block_ref->blkno,
							   old_block_ref->blkno,
							   old_block_ref == new_block_ref ? "true" : "false",
							   offnum,
							   (unsigned int) PageGetMaxOffsetNumber(newpage),
							   new_block_ref->has_image ? "true" : "false",
							   new_block_ref->apply_image ? "true" : "false",
							   new_block_ref->has_data ? "true" : "false",
							   record->init_page ? "true" : "false",
							   xlrec->flags)));

		if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD)
		{
			memcpy(&prefixlen, recdata, sizeof(uint16));
			recdata += sizeof(uint16);
		}
		if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
		{
			memcpy(&suffixlen, recdata, sizeof(uint16));
			recdata += sizeof(uint16);
		}

		memcpy(&xlhdr, recdata, SizeOfHeapHeader);
		recdata += SizeOfHeapHeader;
		tuplen = recdata_end - recdata;

		htup = &tbuf.hdr;
		MemSet(htup, 0, SizeofHeapTupleHeader);
		newp = (char *) htup + SizeofHeapTupleHeader;

		if (prefixlen > 0)
		{
			int len = xlhdr.t_hoff - SizeofHeapTupleHeader;

			memcpy(newp, recdata, len);
			recdata += len;
			newp += len;
			memcpy(newp, (char *) oldtup + oldtup->t_hoff, prefixlen);
			newp += prefixlen;
			len = tuplen - (xlhdr.t_hoff - SizeofHeapTupleHeader);
			memcpy(newp, recdata, len);
			recdata += len;
			newp += len;
		}
		else
		{
			memcpy(newp, recdata, tuplen);
			recdata += tuplen;
			newp += tuplen;
		}

		if (suffixlen > 0)
			memcpy(newp, ((char *) oldtup) + oldtup_len - suffixlen, suffixlen);

		newlen = SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;
		htup->t_infomask2 = xlhdr.t_infomask2;
		htup->t_infomask = xlhdr.t_infomask;
		htup->t_hoff = xlhdr.t_hoff;
		HeapTupleHeaderSetXmin(htup, record->xid);
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
		htup->t_ctid = newtid;
			if (PageAddItem(newpage, (Item) htup, newlen, offnum, true, true) == InvalidOffsetNumber)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("failed to replay heap update"),
						 errdetail("lsn=%X/%08X newblk=%u oldblk=%u sameblk=%s off=%u maxoff=%u lp=%s page_lsn=%X/%08X image=%s apply_image=%s data=%s init_page=%s flags=0x%02X prefixlen=%u suffixlen=%u oldtup_len=%u datalen=%u newlen=%u lower=%u upper=%u exact_free=%u heap_free=%u lp1=%s/%u lp2=%s/%u lp3=%s/%u lp4=%s/%u",
								   LSN_FORMAT_ARGS(record->lsn),
								   new_block_ref->blkno,
								   old_block_ref->blkno,
								   old_block_ref == new_block_ref ? "true" : "false",
								   offnum,
								   (unsigned int) PageGetMaxOffsetNumber(newpage),
								   fb_replay_debug_lp_state(newpage, offnum),
								   LSN_FORMAT_ARGS(new_state->page_lsn),
								   new_block_ref->has_image ? "true" : "false",
								   new_block_ref->apply_image ? "true" : "false",
								   new_block_ref->has_data ? "true" : "false",
								   record->init_page ? "true" : "false",
								   xlrec->flags,
								   (unsigned int) prefixlen,
								   (unsigned int) suffixlen,
								   (unsigned int) oldtup_len,
								   (unsigned int) datalen,
								   (unsigned int) newlen,
								   (unsigned int) ((PageHeader) newpage)->pd_lower,
								   (unsigned int) ((PageHeader) newpage)->pd_upper,
								   (unsigned int) PageGetExactFreeSpace(newpage),
								   (unsigned int) PageGetHeapFreeSpace(newpage),
								   fb_replay_debug_lp_state(newpage, 1),
								   PageGetMaxOffsetNumber(newpage) >= 1 &&
								   ItemIdHasStorage(PageGetItemId(newpage, 1)) ?
								   (unsigned int) ItemIdGetLength(PageGetItemId(newpage, 1)) : 0,
								   fb_replay_debug_lp_state(newpage, 2),
								   PageGetMaxOffsetNumber(newpage) >= 2 &&
								   ItemIdHasStorage(PageGetItemId(newpage, 2)) ?
								   (unsigned int) ItemIdGetLength(PageGetItemId(newpage, 2)) : 0,
								   fb_replay_debug_lp_state(newpage, 3),
								   PageGetMaxOffsetNumber(newpage) >= 3 &&
								   ItemIdHasStorage(PageGetItemId(newpage, 3)) ?
								   (unsigned int) ItemIdGetLength(PageGetItemId(newpage, 3)) : 0,
								   fb_replay_debug_lp_state(newpage, 4),
								   PageGetMaxOffsetNumber(newpage) >= 4 &&
								   ItemIdHasStorage(PageGetItemId(newpage, 4)) ?
								   (unsigned int) ItemIdGetLength(PageGetItemId(newpage, 4)) : 0)));

		if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(newpage);
	}

	old_state->page_lsn = record->end_lsn;
	new_state->page_lsn = record->end_lsn;
	if (FB_LOCATOR_RELNUMBER(new_block_ref->locator) == 77425251 &&
		new_block_ref->blkno == 488294)
		elog(WARNING,
			 "fb_debug blk488294 update after lsn=%X/%08X oldblk=%u old_off=%u new_off=%u same=%s new_maxoff=%u new_lp1=%s new_lp2=%s new_lp3=%s new_lp4=%s",
			 LSN_FORMAT_ARGS(record->end_lsn),
			 old_block_ref->blkno,
			 xlrec->old_offnum,
			 xlrec->new_offnum,
			 old_block_ref == new_block_ref ? "true" : "false",
			 (unsigned int) PageGetMaxOffsetNumber(newpage),
			 fb_replay_debug_lp_state(newpage, 1),
			 fb_replay_debug_lp_state(newpage, 2),
			 fb_replay_debug_lp_state(newpage, 3),
			 fb_replay_debug_lp_state(newpage, 4));
	result->records_replayed++;
	if (record->committed_after_target && new_block_ref->is_main_relation)
		result->target_update_count++;

	if (source != NULL && tupdesc != NULL &&
		record->committed_after_target && new_block_ref->is_main_relation)
	{
		new_tuple = fb_copy_page_tuple(newpage, new_block_ref->blkno, xlrec->new_offnum);
		if (old_tuple != NULL && new_tuple != NULL)
			fb_append_forward_update(info, tupdesc, toast_store, record,
									 old_tuple, new_tuple, result, source,
									 pending_ops);
		else
			elog(WARNING,
				 "fb_debug missing forward update tuple lsn=%X/%08X oldblk=%u newblk=%u old_off=%u new_off=%u old_tuple=%s new_tuple=%s old_lp=%s new_lp=%s old_page_lsn=%X/%08X new_page_lsn=%X/%08X flags=0x%02X hot=%s",
				 LSN_FORMAT_ARGS(record->lsn),
				 old_block_ref->blkno,
				 new_block_ref->blkno,
				 xlrec->old_offnum,
				 xlrec->new_offnum,
				 old_tuple != NULL ? "present" : "missing",
				 new_tuple != NULL ? "present" : "missing",
				 fb_replay_debug_lp_state(oldpage, xlrec->old_offnum),
				 fb_replay_debug_lp_state(newpage, xlrec->new_offnum),
				 LSN_FORMAT_ARGS(old_state->page_lsn),
				 LSN_FORMAT_ARGS(new_state->page_lsn),
				 xlrec->flags,
				 hot_update ? "true" : "false");
	}
	if (new_block_ref->is_toast_relation && toast_store != NULL && toast_tupdesc != NULL)
	{
		if (old_toast_tuple != NULL)
			fb_toast_store_remove_from_tuple(toast_store, toast_tupdesc, old_toast_tuple);
		if (new_page_image_applied)
			fb_toast_store_sync_page(toast_store, toast_tupdesc, newpage, new_block_ref->blkno);
		else
		{
			new_toast_tuple = fb_copy_page_tuple(newpage, new_block_ref->blkno, xlrec->new_offnum);
			if (new_toast_tuple != NULL)
				fb_toast_store_put_from_tuple(toast_store, toast_tupdesc, new_toast_tuple);
		}
	}
}

/*
 * fb_replay_heap_lock
 *    Replay helper.
 */

static void
fb_replay_heap_lock(const FbRecordRef *record,
					FbReplayStore *store,
					FbReplayControl *control,
					FbReplayResult *result)
{
	xl_heap_lock *xlrec = (xl_heap_lock *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	HeapTupleHeader htup;
	bool needs_redo;
	bool ready;

	if (!fb_record_block_has_applicable_image(block_ref) && !record->init_page)
	{
		state = fb_replay_find_existing_block(store, block_ref);
		if (state == NULL || !state->initialized)
		{
			result->records_replayed++;
			return;
		}
	}
	else
		fb_replay_ensure_block_ready(store, block_ref, record, false,
									 control, result, &state, &ready);
	if (fb_record_block_has_applicable_image(block_ref) || record->init_page)
	{
		if (!ready)
			return;
	}

	page = (Page) state->page;
	needs_redo = fb_replay_block_needs_redo(block_ref);
	if (!needs_redo)
	{
		state->page_lsn = record->end_lsn;
		result->records_replayed++;
		return;
	}
	htup = fb_replay_get_tuple_from_page(page, xlrec->offnum, NULL);
	if (htup == NULL)
	{
		state->page_lsn = record->end_lsn;
		result->records_replayed++;
		return;
	}

	htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	fb_fix_infomask_from_infobits(xlrec->infobits_set,
								  &htup->t_infomask, &htup->t_infomask2);
	if (HEAP_XMAX_IS_LOCKED_ONLY(htup->t_infomask))
	{
		HeapTupleHeaderClearHotUpdated(htup);
		ItemPointerSet(&htup->t_ctid, block_ref->blkno, xlrec->offnum);
	}
	HeapTupleHeaderSetXmax(htup, FB_XL_HEAP_LOCK_XMAX(xlrec));
	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

	state->page_lsn = record->end_lsn;
	result->records_replayed++;
}

/*
 * fb_replay_heap2_prune
 *    Replay helper.
 */

static void
fb_replay_heap2_prune(const FbRecordRef *record,
					  FbReplayStore *store,
					  FbReplayControl *control,
					  FbReplayResult *result)
{
#if PG_VERSION_NUM >= 170000
	xl_heap_prune xlrec;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	bool image_applies;
	bool ready;

	image_applies = fb_record_block_has_applicable_image(block_ref);
	if (image_applies)
	{
		state = fb_replay_find_existing_block(store, block_ref);
		if (fb_replay_prune_image_should_preserve_page(record, state, control))
		{
			state->page_lsn = record->end_lsn;
			result->records_replayed++;
			return;
		}
		fb_replay_ensure_block_ready(store, block_ref, record, false,
									 control, result, &state, &ready);
	}
	else if (!record->init_page)
	{
		state = fb_replay_find_existing_block(store, block_ref);
		if (state == NULL || !state->initialized)
		{
			result->records_replayed++;
			return;
		}
	}
	else
		fb_replay_ensure_block_ready(store, block_ref, record, false,
									 control, result, &state, &ready);
	if (image_applies || record->init_page)
	{
		if (!ready)
			return;
	}
	if (image_applies)
	{
		state->page_lsn = record->end_lsn;
		result->records_replayed++;
		return;
	}

	page = (Page) state->page;
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
		block_ref->blkno == 17079)
		fb_replay_debug_log_toast17079_page("prune before", record->lsn, page);
	if (record->main_data != NULL && record->main_data_len >= SizeOfHeapPrune)
		memcpy(&xlrec, record->main_data, SizeOfHeapPrune);
	else
		MemSet(&xlrec, 0, sizeof(xlrec));

	if (block_ref->has_data && block_ref->data_len > 0)
	{
		OffsetNumber redirected_buf[MaxHeapTuplesPerPage * 2];
		OffsetNumber nowdead_buf[MaxHeapTuplesPerPage];
		OffsetNumber nowunused_buf[MaxHeapTuplesPerPage];
		OffsetNumber *redirected;
		OffsetNumber *nowdead;
		OffsetNumber *nowunused;
		OffsetNumber *frz_offsets;
		xlhp_freeze_plan *plans;
		int nredirected;
		int ndead;
		int nunused;
		int nplans;

		heap_xlog_deserialize_prune_and_freeze(block_ref->data, xlrec.flags,
											   &nplans, &plans, &frz_offsets,
											   &nredirected, &redirected,
											   &ndead, &nowdead,
											   &nunused, &nowunused);
		if (nredirected > 0)
			memcpy(redirected_buf, redirected,
				   sizeof(OffsetNumber) * nredirected * 2);
		if (ndead > 0)
			memcpy(nowdead_buf, nowdead, sizeof(OffsetNumber) * ndead);
		if (nunused > 0)
			memcpy(nowunused_buf, nowunused, sizeof(OffsetNumber) * nunused);
		redirected = redirected_buf;
		nowdead = nowdead_buf;
		nowunused = nowunused_buf;
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
			block_ref->blkno == 17079)
			elog(WARNING,
				 "fb_debug toast prune decoded blk=17079 lsn=%X/%08X nredirected=%d ndead=%d nunused=%d dead_first=%u unused_first=%u cleanup_lock=%s",
				 LSN_FORMAT_ARGS(record->lsn),
				 nredirected,
				 ndead,
				 nunused,
				 ndead > 0 ? nowdead[0] : InvalidOffsetNumber,
				 nunused > 0 ? nowunused[0] : InvalidOffsetNumber,
				 (xlrec.flags & XLHP_CLEANUP_LOCK) != 0 ? "true" : "false");
		fb_replay_prune_apply_future_guard(control,
										   redirected,
										   &nredirected,
										   nowdead,
										   &ndead,
										   nowunused,
										   &nunused);
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
			block_ref->blkno == 17079)
			elog(WARNING,
				 "fb_debug toast prune guarded blk=17079 lsn=%X/%08X recidx=%u nredirected=%d ndead=%d nunused=%d dead_first=%u unused_first=%u",
				 LSN_FORMAT_ARGS(record->lsn),
				 control != NULL ? control->record_index : 0,
				 nredirected,
				 ndead,
				 nunused,
				 ndead > 0 ? nowdead[0] : InvalidOffsetNumber,
				 nunused > 0 ? nowunused[0] : InvalidOffsetNumber);
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395737 &&
			(block_ref->blkno == 1084485 ||
			 block_ref->blkno == 1084491 ||
			 block_ref->blkno == 38724))
			elog(WARNING,
				 "fb_debug prune apply blk=%u lsn=%X/%08X before_lp1=%s before_lp4=%s ndead=%d nunused=%d dead_first=%u unused_first=%u",
				 block_ref->blkno,
				 LSN_FORMAT_ARGS(record->lsn),
				 fb_replay_debug_lp_state(page, FirstOffsetNumber),
				 fb_replay_debug_lp_state(page, 4),
				 ndead,
				 nunused,
				 ndead > 0 ? nowdead[0] : InvalidOffsetNumber,
				 nunused > 0 ? nowunused[0] : InvalidOffsetNumber);
		if (nredirected > 0 || ndead > 0 || nunused > 0)
		{
			fb_page_prune_execute(page,
								  (xlrec.flags & XLHP_CLEANUP_LOCK) == 0,
								  redirected, nredirected,
								  nowdead, ndead,
								  nowunused, nunused);
		}

		for (int p = 0; p < nplans; p++)
		{
			HeapTupleFreeze frz;

			frz.xmax = plans[p].xmax;
			frz.t_infomask2 = plans[p].t_infomask2;
			frz.t_infomask = plans[p].t_infomask;
			frz.frzflags = plans[p].frzflags;
			frz.offset = InvalidOffsetNumber;

			for (int i = 0; i < plans[p].ntuples; i++)
			{
				OffsetNumber offset = *(frz_offsets++);
				ItemId lp = PageGetItemId(page, offset);
				HeapTupleHeader tuple = (HeapTupleHeader) PageGetItem(page, lp);

				heap_execute_freeze_tuple(tuple, &frz);
			}
		}
	}

	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395737 &&
		(block_ref->blkno == 1084485 ||
		 block_ref->blkno == 1084491 ||
		 block_ref->blkno == 38724))
		elog(WARNING,
			 "fb_debug prune done blk=%u lsn=%X/%08X after_lp1=%s after_lp4=%s maxoff=%u",
			 block_ref->blkno,
			 LSN_FORMAT_ARGS(record->lsn),
			 fb_replay_debug_lp_state(page, FirstOffsetNumber),
			 fb_replay_debug_lp_state(page, 4),
			 (unsigned int) PageGetMaxOffsetNumber(page));
	if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
		block_ref->blkno == 17079)
		fb_replay_debug_log_toast17079_page("prune after", record->end_lsn, page);

	state->page_lsn = record->end_lsn;
	result->records_replayed++;
#else
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	bool ready;

	if (!fb_record_block_has_applicable_image(block_ref) && !record->init_page)
	{
		state = fb_replay_find_existing_block(store, block_ref);
		if (state == NULL || !state->initialized)
		{
			result->records_replayed++;
			return;
		}
	}
	else
		fb_replay_ensure_block_ready(store, block_ref, record, false,
									 control, result, &state, &ready);
	if (fb_record_block_has_applicable_image(block_ref) || record->init_page)
	{
		if (!ready)
			return;
	}

	/*
	 * PG12-16 use older prune record layouts. Keep them as the existing
	 * conservative no-op until those versions get dedicated minimal replay.
	 */
	state->page_lsn = record->end_lsn;
	result->records_replayed++;
#endif
}

/*
 * fb_replay_heap_confirm
 *    Replay helper.
 */

static void
fb_replay_heap_confirm(const FbRecordRef *record,
					   FbReplayStore *store,
					   FbReplayControl *control,
					   FbReplayResult *result)
{
	xl_heap_confirm *xlrec = (xl_heap_confirm *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	HeapTupleHeader htup;
	bool needs_redo;
	bool ready;

	fb_replay_ensure_block_ready(store, block_ref, record, false,
								 control, result, &state, &ready);
	if (!ready)
		return;
	page = (Page) state->page;
	needs_redo = fb_replay_block_needs_redo(block_ref);
	if (!needs_redo)
	{
		state->page_lsn = record->end_lsn;
		result->records_replayed++;
		return;
	}
	htup = fb_replay_get_old_tuple_from_page(page, xlrec->offnum, NULL);
	if (htup == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("failed to locate tuple for heap confirm redo")));

	ItemPointerSet(&htup->t_ctid, block_ref->blkno, xlrec->offnum);
	state->page_lsn = record->end_lsn;
	result->records_replayed++;
}

/*
 * fb_replay_heap_inplace
 *    Replay helper.
 */

static void
fb_replay_heap_inplace(const FbRecordRef *record,
					   FbReplayStore *store,
					   FbReplayControl *control,
					   FbReplayResult *result)
{
	xl_heap_inplace *xlrec = (xl_heap_inplace *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	HeapTupleHeader htup;
	uint32 oldlen;
	bool needs_redo;
	bool ready;

	fb_replay_ensure_block_ready(store, block_ref, record, false,
								 control, result, &state, &ready);
	if (!ready)
		return;
	page = (Page) state->page;
	needs_redo = fb_replay_block_needs_redo(block_ref);
	if (!needs_redo)
	{
		state->page_lsn = record->end_lsn;
		result->records_replayed++;
		return;
	}
	htup = fb_replay_get_old_tuple_from_page(page, xlrec->offnum, NULL);
	if (htup == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("failed to locate tuple for heap inplace redo")));
	if (!block_ref->has_data || block_ref->data_len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("heap inplace record missing block data")));

	oldlen = ItemIdGetLength(PageGetItemId(page, xlrec->offnum)) - htup->t_hoff;
	if (oldlen != block_ref->data_len)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("wrong tuple length during heap inplace redo")));

	memcpy((char *) htup + htup->t_hoff, block_ref->data, block_ref->data_len);
	state->page_lsn = record->end_lsn;
	result->records_replayed++;
}

/*
 * fb_replay_heap2_visible
 *    Replay helper.
 */

static void
fb_replay_heap2_visible(const FbRecordRef *record,
						FbReplayStore *store,
						FbReplayControl *control,
						FbReplayResult *result)
{
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	bool needs_redo;
	bool ready;

	fb_replay_ensure_block_ready(store, block_ref, record, false,
								 control, result, &state, &ready);
	if (!ready)
		return;
	page = (Page) state->page;
	needs_redo = fb_replay_block_needs_redo(block_ref);
	if (!needs_redo)
	{
		state->page_lsn = record->end_lsn;
		result->records_replayed++;
		return;
	}
	PageSetAllVisible(page);
	state->page_lsn = record->end_lsn;
	result->records_replayed++;
}

/*
 * fb_replay_heap2_lock_updated
 *    Replay helper.
 */

static void
fb_replay_heap2_lock_updated(const FbRecordRef *record,
							 FbReplayStore *store,
							 FbReplayControl *control,
							 FbReplayResult *result)
{
	xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	HeapTupleHeader htup;
	bool needs_redo;
	bool ready;

	if (!fb_record_block_has_applicable_image(block_ref) && !record->init_page)
	{
		state = fb_replay_find_existing_block(store, block_ref);
		if (state == NULL || !state->initialized)
		{
			result->records_replayed++;
			return;
		}
	}
	else
		fb_replay_ensure_block_ready(store, block_ref, record, false,
									 control, result, &state, &ready);
	if (fb_record_block_has_applicable_image(block_ref) || record->init_page)
	{
		if (!ready)
			return;
	}

	page = (Page) state->page;
	needs_redo = fb_replay_block_needs_redo(block_ref);
	if (!needs_redo)
	{
		state->page_lsn = record->end_lsn;
		result->records_replayed++;
		return;
	}
	htup = fb_replay_get_tuple_from_page(page, xlrec->offnum, NULL);
	if (htup == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("failed to locate tuple for heap lock_updated redo")));

	htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	fb_fix_infomask_from_infobits(xlrec->infobits_set,
								  &htup->t_infomask, &htup->t_infomask2);
	HeapTupleHeaderSetXmax(htup, xlrec->xmax);
	state->page_lsn = record->end_lsn;
	result->records_replayed++;
}

/*
 * fb_replay_xlog_fpi
 *    Replay helper.
 */

static void
fb_replay_xlog_fpi(const FbRecordRef *record,
				   FbReplayStore *store,
				   FbReplayControl *control,
				   FbReplayResult *result,
				   TupleDesc toast_tupdesc,
				   FbToastStore *toast_store)
{
	int block_index;

	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		FbReplayBlockState *state;
		FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[block_index];
		bool ready;

		fb_replay_ensure_block_ready(store, block_ref, record, false,
									 control, result, &state, &ready);
		if (!ready)
			return;
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395737 &&
			(block_ref->blkno == 1084485 || block_ref->blkno == 1084491))
			elog(WARNING,
				 "fb_debug fpi apply blk=%u lsn=%X/%08X lp1=%s lp4=%s maxoff=%u",
				 block_ref->blkno,
				 LSN_FORMAT_ARGS(record->lsn),
				 fb_replay_debug_lp_state((Page) state->page, FirstOffsetNumber),
				 fb_replay_debug_lp_state((Page) state->page, 4),
				 (unsigned int) PageGetMaxOffsetNumber((Page) state->page));
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
			block_ref->blkno == 17079)
			elog(WARNING,
				 "fb_debug toast fpi apply blk=%u lsn=%X/%08X maxoff=%u lower=%u upper=%u exact_free=%u heap_free=%u lp23=%s lp24=%s lp41=%s",
				 block_ref->blkno,
				 LSN_FORMAT_ARGS(record->lsn),
				 (unsigned int) PageGetMaxOffsetNumber((Page) state->page),
				 (unsigned int) ((PageHeader) state->page)->pd_lower,
				 (unsigned int) ((PageHeader) state->page)->pd_upper,
				 (unsigned int) PageGetExactFreeSpace((Page) state->page),
				 (unsigned int) PageGetHeapFreeSpace((Page) state->page),
				 fb_replay_debug_lp_state((Page) state->page, 23),
				 fb_replay_debug_lp_state((Page) state->page, 24),
				 fb_replay_debug_lp_state((Page) state->page, 41));
		if (FB_LOCATOR_RELNUMBER(block_ref->locator) == 16395804 &&
			block_ref->blkno == 26273)
			fb_replay_debug_log_toast26273_page("fpi apply", record->lsn,
												  (Page) state->page);
		state->page_lsn = record->end_lsn;
		if (block_ref->is_toast_relation && toast_store != NULL && toast_tupdesc != NULL)
			fb_toast_store_sync_page(toast_store, toast_tupdesc,
									 (Page) state->page, block_ref->blkno);
	}

	result->records_replayed++;
}

/*
 * fb_replay_heap2_multi_insert
 *    Replay helper.
 */

static void
fb_replay_heap2_multi_insert(const FbRelationInfo *info,
							 const FbRecordRef *record,
							 FbReplayStore *store,
							 TupleDesc tupdesc,
							 TupleDesc toast_tupdesc,
							 FbToastStore *toast_store,
							 FbReplayControl *control,
							 FbReplayResult *result,
							 FbReverseOpSource *source,
							 FbPendingReverseOpQueue *pending_ops)
{
	xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	char *tupdata;
	char *endptr;
	int i;
	bool isinit = record->init_page;
	bool image_applied;
	Size len;

	union
	{
		HeapTupleHeaderData hdr;
		char data[MaxHeapTupleSize];
	} tbuf;
	bool ready;

	fb_replay_ensure_block_ready(store, block_ref, record, isinit,
								 control, result, &state, &ready);
	if (!ready)
		return;
	page = (Page) state->page;
	image_applied = fb_record_block_has_applicable_image(block_ref);

	if (!image_applied)
	{
		if (!block_ref->has_data || block_ref->data_len == 0)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("heap multi insert record missing block data")));

		tupdata = block_ref->data;
		len = block_ref->data_len;
		endptr = tupdata + len;
	}

	for (i = 0; i < xlrec->ntuples; i++)
	{
		OffsetNumber offnum;
		ItemPointerData tid;

		if (isinit)
			offnum = FirstOffsetNumber + i;
		else
			offnum = xlrec->offsets[i];

		if (!image_applied)
		{
			xl_multi_insert_tuple *xlhdr;
			HeapTupleHeader htup;
			uint32 newlen;

			xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(tupdata);
			tupdata = ((char *) xlhdr) + SizeOfMultiInsertTuple;

			newlen = xlhdr->datalen;
			htup = &tbuf.hdr;
			MemSet(htup, 0, SizeofHeapTupleHeader);
			memcpy((char *) htup + SizeofHeapTupleHeader, tupdata, newlen);
			tupdata += newlen;

			newlen += SizeofHeapTupleHeader;
			htup->t_infomask2 = xlhdr->t_infomask2;
			htup->t_infomask = xlhdr->t_infomask;
			htup->t_hoff = xlhdr->t_hoff;
			HeapTupleHeaderSetXmin(htup, record->xid);
			HeapTupleHeaderSetCmin(htup, FirstCommandId);
			ItemPointerSetBlockNumber(&tid, block_ref->blkno);
			ItemPointerSetOffsetNumber(&tid, offnum);
			htup->t_ctid = tid;

			if (!fb_replay_expand_unused_slots(page, offnum))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("failed to prepare heap multi insert line pointers"),
						 errdetail("phase=%s recidx=%u lsn=%X/%08X rel=%u/%u/%u blk=%u off=%u tuple_index=%d image=%s apply_image=%s data=%s init_page=%s maxoff=%u page_lsn=%X/%08X lp=%s lower=%u upper=%u exact_free=%u heap_free=%u",
								   control != NULL &&
								   control->phase == FB_REPLAY_PHASE_FINAL ? "final" :
								   control != NULL &&
								   control->phase == FB_REPLAY_PHASE_WARM ? "warm" :
								   control != NULL &&
								   control->phase == FB_REPLAY_PHASE_DISCOVER ? "discover" :
								   "unknown",
								   control != NULL ? control->record_index : 0,
								   LSN_FORMAT_ARGS(record->lsn),
								   FB_LOCATOR_SPCOID(block_ref->locator),
								   FB_LOCATOR_DBOID(block_ref->locator),
								   FB_LOCATOR_RELNUMBER(block_ref->locator),
								   block_ref->blkno,
								   offnum,
								   i,
								   block_ref->has_image ? "true" : "false",
								   block_ref->apply_image ? "true" : "false",
								   block_ref->has_data ? "true" : "false",
								   record->init_page ? "true" : "false",
								   (unsigned int) PageGetMaxOffsetNumber(page),
								   LSN_FORMAT_ARGS(state->page_lsn),
								   fb_replay_debug_lp_state(page, offnum),
								   (unsigned int) ((PageHeader) page)->pd_lower,
								   (unsigned int) ((PageHeader) page)->pd_upper,
								   (unsigned int) PageGetExactFreeSpace(page),
								   (unsigned int) PageGetHeapFreeSpace(page))));
			if (PageGetMaxOffsetNumber(page) >= offnum &&
				(ItemIdIsUsed(PageGetItemId(page, offnum)) ||
				 ItemIdHasStorage(PageGetItemId(page, offnum))))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("heap multi insert target slot already used"),
						 errdetail("phase=%s recidx=%u lsn=%X/%08X rel=%u/%u/%u blk=%u off=%u tuple_index=%d image=%s apply_image=%s data=%s init_page=%s maxoff=%u page_lsn=%X/%08X lp=%s used=%s has_storage=%s lower=%u upper=%u exact_free=%u heap_free=%u",
								   control != NULL &&
								   control->phase == FB_REPLAY_PHASE_FINAL ? "final" :
								   control != NULL &&
								   control->phase == FB_REPLAY_PHASE_WARM ? "warm" :
								   control != NULL &&
								   control->phase == FB_REPLAY_PHASE_DISCOVER ? "discover" :
								   "unknown",
								   control != NULL ? control->record_index : 0,
								   LSN_FORMAT_ARGS(record->lsn),
								   FB_LOCATOR_SPCOID(block_ref->locator),
								   FB_LOCATOR_DBOID(block_ref->locator),
								   FB_LOCATOR_RELNUMBER(block_ref->locator),
								   block_ref->blkno,
								   offnum,
								   i,
								   block_ref->has_image ? "true" : "false",
								   block_ref->apply_image ? "true" : "false",
								   block_ref->has_data ? "true" : "false",
								   record->init_page ? "true" : "false",
								   (unsigned int) PageGetMaxOffsetNumber(page),
								   LSN_FORMAT_ARGS(state->page_lsn),
								   fb_replay_debug_lp_state(page, offnum),
								   ItemIdIsUsed(PageGetItemId(page, offnum)) ? "true" : "false",
								   ItemIdHasStorage(PageGetItemId(page, offnum)) ? "true" : "false",
								   (unsigned int) ((PageHeader) page)->pd_lower,
								   (unsigned int) ((PageHeader) page)->pd_upper,
								   (unsigned int) PageGetExactFreeSpace(page),
								   (unsigned int) PageGetHeapFreeSpace(page))));
			if (PageAddItem(page, (Item) htup, newlen, offnum, true, true) == InvalidOffsetNumber)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("failed to replay heap multi insert"),
						 errdetail("phase=%s recidx=%u lsn=%X/%08X rel=%u/%u/%u blk=%u off=%u tuple_index=%d image=%s apply_image=%s data=%s init_page=%s maxoff=%u has_item=%s page_lsn=%X/%08X lp=%s lower=%u upper=%u exact_free=%u heap_free=%u",
								   control != NULL &&
								   control->phase == FB_REPLAY_PHASE_FINAL ? "final" :
								   control != NULL &&
								   control->phase == FB_REPLAY_PHASE_WARM ? "warm" :
								   control != NULL &&
								   control->phase == FB_REPLAY_PHASE_DISCOVER ? "discover" :
								   "unknown",
								   control != NULL ? control->record_index : 0,
								   LSN_FORMAT_ARGS(record->lsn),
								   FB_LOCATOR_SPCOID(block_ref->locator),
								   FB_LOCATOR_DBOID(block_ref->locator),
								   FB_LOCATOR_RELNUMBER(block_ref->locator),
								   block_ref->blkno,
								   offnum,
								   i,
								   block_ref->has_image ? "true" : "false",
								   block_ref->apply_image ? "true" : "false",
								   block_ref->has_data ? "true" : "false",
								   record->init_page ? "true" : "false",
								   (unsigned int) PageGetMaxOffsetNumber(page),
								   (PageGetMaxOffsetNumber(page) >= offnum &&
									ItemIdIsUsed(PageGetItemId(page, offnum))) ? "true" : "false",
								   LSN_FORMAT_ARGS(state->page_lsn),
								   fb_replay_debug_lp_state(page, offnum),
								   (unsigned int) ((PageHeader) page)->pd_lower,
								   (unsigned int) ((PageHeader) page)->pd_upper,
								   (unsigned int) PageGetExactFreeSpace(page),
								   (unsigned int) PageGetHeapFreeSpace(page))));
		}

			if (block_ref->is_toast_relation && toast_store != NULL && toast_tupdesc != NULL)
			{
				if (image_applied)
					fb_toast_store_sync_page(toast_store, toast_tupdesc, page, block_ref->blkno);
				else
				{
					HeapTuple toast_tuple = fb_copy_page_tuple(page, block_ref->blkno, offnum);

					fb_toast_store_put_from_tuple(toast_store, toast_tupdesc, toast_tuple);
				}
			}

		if (source != NULL && tupdesc != NULL &&
			record->committed_after_target && block_ref->is_main_relation)
		{
			HeapTuple new_tuple = fb_copy_page_tuple(page, block_ref->blkno, offnum);

			if (new_tuple != NULL)
				fb_append_forward_insert(info, tupdesc, toast_store, record, new_tuple,
										result, source, pending_ops);
		}
	}

	if (!image_applied && tupdata != endptr)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("heap multi insert tuple length mismatch")));

	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
		PageClearAllVisible(page);
	if (xlrec->flags & FB_XLH_INSERT_ALL_FROZEN_SET)
		PageSetAllVisible(page);

	state->page_lsn = record->end_lsn;
	result->records_replayed++;
	if (record->committed_after_target && block_ref->is_main_relation)
		result->target_insert_count += xlrec->ntuples;
}

/*
 * fb_replay_run_pass
 *    Replay helper.
 */

static void
fb_replay_apply_record(const FbRelationInfo *info,
					   const FbRecordRef *record,
					   FbReplayStore *store,
					   TupleDesc tupdesc,
					   TupleDesc toast_tupdesc,
					   FbToastStore *toast_store,
					   FbReplayControl *control,
					   FbReplayResult *result,
					   FbReverseOpSource *source,
					   FbPendingReverseOpQueue *pending_ops)
{
	switch (record->kind)
	{
		case FB_WAL_RECORD_HEAP_INSERT:
			fb_replay_heap_insert(info, record, store, tupdesc, toast_tupdesc,
								  toast_store, control, result, source,
								  pending_ops);
			break;
		case FB_WAL_RECORD_HEAP_DELETE:
			fb_replay_heap_delete(info, record, store, tupdesc, toast_tupdesc,
								  toast_store, control, result, source,
								  pending_ops);
			break;
		case FB_WAL_RECORD_HEAP_UPDATE:
			fb_replay_heap_update(info, record, store, false, tupdesc, toast_tupdesc,
								  toast_store, control, result, source,
								  pending_ops);
			break;
		case FB_WAL_RECORD_HEAP_HOT_UPDATE:
			fb_replay_heap_update(info, record, store, true, tupdesc, toast_tupdesc,
								  toast_store, control, result, source,
								  pending_ops);
			break;
		case FB_WAL_RECORD_HEAP_CONFIRM:
			fb_replay_heap_confirm(record, store, control, result);
			break;
		case FB_WAL_RECORD_HEAP_LOCK:
			fb_replay_heap_lock(record, store, control, result);
			break;
		case FB_WAL_RECORD_HEAP_INPLACE:
			fb_replay_heap_inplace(record, store, control, result);
			break;
		case FB_WAL_RECORD_HEAP2_PRUNE:
			fb_replay_heap2_prune(record, store, control, result);
			break;
		case FB_WAL_RECORD_HEAP2_VISIBLE:
			fb_replay_heap2_visible(record, store, control, result);
			break;
		case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
			fb_replay_heap2_multi_insert(info, record, store, tupdesc, toast_tupdesc,
										 toast_store, control, result, source,
										 pending_ops);
			break;
		case FB_WAL_RECORD_HEAP2_LOCK_UPDATED:
			fb_replay_heap2_lock_updated(record, store, control, result);
			break;
		case FB_WAL_RECORD_XLOG_FPI:
		case FB_WAL_RECORD_XLOG_FPI_FOR_HINT:
			fb_replay_xlog_fpi(record, store, control, result, toast_tupdesc, toast_store);
			break;
	}
}

static void
fb_replay_run_pass(const FbRelationInfo *info,
				   const FbWalRecordIndex *index,
				   TupleDesc tupdesc,
				   TupleDesc toast_tupdesc,
				   FbToastStore *toast_store,
				   FbReplayStore *store,
				   FbReplayControl *control,
				   FbReplayResult *result,
				   FbReverseOpSource *source,
				   FbPendingReverseOpQueue *pending_ops,
				   uint32 start_record_index,
				   XLogRecPtr lower_bound,
				   XLogRecPtr upper_bound)
{
	FbWalRecordCursor *cursor;
	FbRecordRef record;
	uint32 record_index;
	uint32 progress_stride = 1;
	bool have_prev = false;
	XLogRecPtr prev_record_lsn = InvalidXLogRecPtr;

	if (index != NULL && index->record_count > 0)
		progress_stride = Max((uint32) 1, index->record_count / 1024);

	cursor = fb_wal_record_cursor_open(index, FB_SPOOL_FORWARD);
	if (start_record_index > 0 &&
		!fb_wal_record_cursor_seek(cursor, start_record_index))
		elog(ERROR, "failed to seek WAL cursor to record %u", start_record_index);
	while (fb_wal_record_cursor_read(cursor, &record, &record_index))
	{
		/*
		 * Summary locator replay can surface the same WAL record twice as
		 * adjacent cursor entries. WAL record start LSNs are unique, so exact
		 * repeated starts are safe to drop here.
		 */
		if (have_prev &&
			!XLogRecPtrIsInvalid(prev_record_lsn) &&
			record.lsn == prev_record_lsn)
			continue;

		have_prev = true;
		prev_record_lsn = record.lsn;

		if (!XLogRecPtrIsInvalid(lower_bound) && record.lsn < lower_bound)
			continue;
		if (!XLogRecPtrIsInvalid(upper_bound) && record.lsn >= upper_bound)
			continue;
	if (control != NULL)
	{
		control->index = index;
		control->record_index = record_index;
		if (record_index == 0 ||
			(record_index + 1) >= index->record_count ||
				((record_index + 1) % progress_stride) == 0)
				fb_replay_progress_update(control,
										 (uint64) record_index + 1,
										 index->record_count);
		}
		if (control != NULL &&
			control->phase == FB_REPLAY_PHASE_DISCOVER &&
			fb_replay_record_touches_missing_block(control, &record))
		{
			fb_replay_note_record_missing_blocks(control, &record);
			continue;
		}
		if (control != NULL &&
			control->phase == FB_REPLAY_PHASE_WARM &&
			!fb_record_should_apply_for_backtrack(&record,
												 control->missing_blocks))
			continue;
		fb_replay_apply_record(info, &record, store, tupdesc, toast_tupdesc,
							   toast_store, control, result, source,
							   pending_ops);
	}
	fb_wal_record_cursor_close(cursor);
}

static bool
fb_replay_record_can_anchor_block(const FbRecordRef *record,
								  int block_index)
{
	if (record == NULL ||
		block_index < 0 ||
		block_index >= record->block_count)
		return false;

	if (fb_record_block_has_applicable_image(&record->blocks[block_index]))
		return true;

	return fb_record_allows_init_for_block(record, block_index);
}

static bool
fb_replay_warm_store(const FbRelationInfo *info,
					 const FbWalRecordIndex *index,
					 TupleDesc toast_tupdesc,
					 FbToastStore *toast_store,
					 FbReplayStore *store,
					 HTAB *backtrack_blocks,
					 FbReplayResult *result)
{
	XLogRecPtr min_anchor_lsn;
	XLogRecPtr warm_end_lsn;
	FbReplayRawWarmVisitorState raw_state;
	FbReplayControl warm_control;
	HTAB *warm_missing = NULL;

	if (fb_replay_missing_block_count(backtrack_blocks) == 0)
	{
		fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_WARM, 100, NULL);
		return false;
	}

	min_anchor_lsn = fb_replay_min_anchor_lsn(backtrack_blocks);
	if (XLogRecPtrIsInvalid(min_anchor_lsn))
	{
		fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_WARM, 100, NULL);
		return false;
	}

	warm_end_lsn = fb_replay_missing_blocks_end_lsn(backtrack_blocks);
	if (XLogRecPtrIsInvalid(warm_end_lsn) || warm_end_lsn <= min_anchor_lsn)
	{
		fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_WARM, 100, NULL);
		return false;
	}


	MemSet(&raw_state, 0, sizeof(raw_state));
	MemSet(&warm_control, 0, sizeof(warm_control));
	raw_state.info = info;
	raw_state.store = store;
	raw_state.toast_tupdesc = toast_tupdesc;
	raw_state.toast_store = toast_store;
	raw_state.apply_blocks = backtrack_blocks;
	raw_state.result = result;
	warm_missing = fb_replay_create_missing_block_hash();
	warm_control.phase = FB_REPLAY_PHASE_DISCOVER;
	warm_control.round_no = 1;
	warm_control.missing_blocks = warm_missing;
	raw_state.control = &warm_control;

	fb_wal_visit_resolved_records(index,
								  min_anchor_lsn,
								  warm_end_lsn,
								  fb_replay_raw_warm_visitor,
								  &raw_state);
	if (fb_replay_missing_block_count(warm_missing) > 0)
	{
		FbReplayResult warm_missing_result;

		fb_replay_init_result(index, &warm_missing_result);
		fb_replay_resolve_missing_anchors(info, index, warm_missing,
										  &warm_missing_result);
		result->summary_anchor_hits += warm_missing_result.summary_anchor_hits;
		result->summary_anchor_fallback += warm_missing_result.summary_anchor_fallback;
		result->summary_anchor_segments_read +=
			warm_missing_result.summary_anchor_segments_read;
		fb_replay_merge_missing_blocks(backtrack_blocks, warm_missing);
		hash_destroy(warm_missing);
		fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_WARM, 100, NULL);
		return true;
	}
	hash_destroy(warm_missing);
	fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_WARM, 100, NULL);
	return false;
}

/*
 * fb_replay_init_result
 *    Replay helper.
 */

static void
fb_replay_init_result(const FbWalRecordIndex *index,
					  FbReplayResult *result)
{
	MemSet(result, 0, sizeof(*result));
	result->tracked_bytes = index->tracked_bytes;
	result->memory_limit_bytes = index->memory_limit_bytes;
}

FbReplayDiscoverState *
fb_replay_discover(const FbRelationInfo *info,
				   const FbWalRecordIndex *index)
{
	FbReplayDiscoverState *state;
	uint32 iteration;

	state = palloc0(sizeof(*state));
	state->toast_tupdesc = fb_replay_open_toast_tupdesc(info);
	state->backtrack_blocks = fb_replay_create_missing_block_hash();

	if (index != NULL && index->precomputed_missing_blocks != NULL)
	{
		state->precomputed_missing_blocks = index->precomputed_missing_block_count;
		fb_progress_enter_stage(FB_PROGRESS_STAGE_REPLAY_DISCOVER, "precomputed");
		if (index->precomputed_missing_block_count > 0)
		{
			FbReplayResult discover_result;

			fb_replay_seed_missing_blocks_from_index(state->backtrack_blocks, index);
			fb_replay_init_result(index, &discover_result);
			fb_replay_resolve_missing_anchors(info, index, state->backtrack_blocks,
											  &discover_result);
			state->summary_anchor_hits = discover_result.summary_anchor_hits;
			state->summary_anchor_fallback = discover_result.summary_anchor_fallback;
			state->summary_anchor_segments_read =
				discover_result.summary_anchor_segments_read;
		}
		else
		{
			state->discover_skipped = true;
			fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_DISCOVER, 100,
									   "precomputed");
			return state;
		}
	}

	for (iteration = 0; iteration < 8; iteration++)
	{
		MemoryContext discover_ctx;
		MemoryContext oldctx;
		FbReplayStore discover_store;
		FbReplayControl discover_control;
		FbReplayResult discover_result;
		FbToastStore *discover_toast_store = NULL;
		HTAB *iteration_missing;

		discover_ctx = AllocSetContextCreate(CurrentMemoryContext,
											 "fb replay discover",
											 ALLOCSET_DEFAULT_SIZES);
		oldctx = MemoryContextSwitchTo(discover_ctx);
		MemSet(&discover_store, 0, sizeof(discover_store));
		discover_store.blocks = fb_replay_create_block_hash();
		if (state->toast_tupdesc != NULL)
			discover_toast_store = fb_toast_store_create();
		fb_replay_init_result(index, &discover_result);
		MemoryContextSwitchTo(oldctx);

		if (fb_replay_warm_store(info, index, state->toast_tupdesc, discover_toast_store,
								 &discover_store, state->backtrack_blocks, &discover_result))
		{
			if (discover_toast_store != NULL)
				fb_toast_store_destroy(discover_toast_store);
			MemoryContextDelete(discover_ctx);
			if (iteration == 7)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("too many shared backtracking rounds while resolving missing FPI")));
			continue;
		}

		iteration_missing = fb_replay_create_missing_block_hash();
		MemSet(&discover_control, 0, sizeof(discover_control));
		discover_control.phase = FB_REPLAY_PHASE_DISCOVER;
		discover_control.round_no = iteration + 1;
		discover_control.missing_blocks = iteration_missing;
		state->discover_rounds = iteration + 1;
		fb_replay_progress_enter(&discover_control);
		fb_replay_run_pass(info, index, NULL, state->toast_tupdesc, discover_toast_store,
						   &discover_store, &discover_control, &discover_result,
						   NULL, NULL, 0, index->anchor_redo_lsn, InvalidXLogRecPtr);
		{
			char *detail = psprintf("round=%u", iteration + 1);

			fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_DISCOVER, 100, detail);
			pfree(detail);
		}

		if (fb_replay_missing_block_count(iteration_missing) == 0)
		{
			hash_destroy(iteration_missing);
			if (discover_toast_store != NULL)
				fb_toast_store_destroy(discover_toast_store);
			MemoryContextDelete(discover_ctx);
			break;
		}

		fb_replay_resolve_missing_anchors(info, index, iteration_missing, &discover_result);
		state->summary_anchor_hits += discover_result.summary_anchor_hits;
		state->summary_anchor_fallback += discover_result.summary_anchor_fallback;
		state->summary_anchor_segments_read +=
			discover_result.summary_anchor_segments_read;
		fb_replay_raise_unresolved_missing_fpi(index, iteration_missing);
		fb_replay_merge_missing_blocks(state->backtrack_blocks, iteration_missing);
		hash_destroy(iteration_missing);
		if (discover_toast_store != NULL)
			fb_toast_store_destroy(discover_toast_store);
		MemoryContextDelete(discover_ctx);

		if (iteration == 7)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("too many shared backtracking rounds while resolving missing FPI")));
	}

	return state;
}

void
fb_replay_discover_destroy(FbReplayDiscoverState *state)
{
	if (state == NULL)
		return;

	if (state->backtrack_blocks != NULL)
		hash_destroy(state->backtrack_blocks);
	if (state->toast_tupdesc != NULL)
		FreeTupleDesc(state->toast_tupdesc);
	pfree(state);
}

FbReplayWarmState *
fb_replay_warm(const FbRelationInfo *info,
			   const FbWalRecordIndex *index,
			   const FbReplayDiscoverState *discover,
			   FbReplayResult *result)
{
	FbReplayWarmState *state;

	state = palloc0(sizeof(*state));
	state->toast_tupdesc = (discover != NULL) ? discover->toast_tupdesc : NULL;
	state->store.blocks = fb_replay_create_block_hash();
	fb_replay_init_result(index, result);
	if (state->toast_tupdesc != NULL)
		state->toast_store = fb_toast_store_create();

	fb_progress_enter_stage(FB_PROGRESS_STAGE_REPLAY_WARM, NULL);
	while (fb_replay_warm_store(info, index, state->toast_tupdesc, state->toast_store,
								&state->store,
								(discover != NULL) ? discover->backtrack_blocks : NULL,
								result))
	{
		hash_destroy(state->store.blocks);
		state->store.blocks = fb_replay_create_block_hash();
		if (state->toast_store != NULL)
		{
			fb_toast_store_destroy(state->toast_store);
			state->toast_store = fb_toast_store_create();
		}
	}
	state->warm_tracked_bytes = Max(index->tracked_bytes, result->tracked_bytes);
	return state;
}

void
fb_replay_warm_destroy(FbReplayWarmState *state)
{
	if (state == NULL)
		return;

	if (state->toast_store != NULL)
		fb_toast_store_destroy(state->toast_store);
	if (state->store.blocks != NULL)
		hash_destroy(state->store.blocks);
	pfree(state);
}

void
fb_replay_final_build_reverse_source(const FbRelationInfo *info,
									 const FbWalRecordIndex *index,
									 TupleDesc tupdesc,
									 const FbReplayWarmState *warm,
									 FbReplayResult *result,
									 FbReverseOpSource *source)
{
	FbReplayControl final_control;
	FbPendingReverseOpQueue pending_ops;

	if (warm == NULL)
		elog(ERROR, "FbReplayWarmState must not be NULL");

	fb_replay_init_result(index, result);
	result->tracked_bytes = warm->warm_tracked_bytes;

	MemSet(&final_control, 0, sizeof(final_control));
	MemSet(&pending_ops, 0, sizeof(pending_ops));
	final_control.phase = FB_REPLAY_PHASE_FINAL;
	fb_replay_build_prune_lookahead(index, &final_control.prune_lookahead);
	fb_replay_progress_enter(&final_control);
	fb_replay_run_pass(info, index, tupdesc, warm->toast_tupdesc, warm->toast_store,
					   (FbReplayStore *) &warm->store, &final_control, result,
					   source, &pending_ops, 0, index->anchor_redo_lsn,
					   InvalidXLogRecPtr);
	fb_pending_reverse_op_flush(&pending_ops, info, tupdesc, warm->toast_store,
								result, source);
	if (final_control.prune_lookahead != NULL)
		hash_destroy(final_control.prune_lookahead);
	fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_FINAL, 100, NULL);
}

/*
 * fb_replay_execute_internal
 *    Replay helper.
 */

static void
fb_replay_execute_internal(const FbRelationInfo *info,
						   const FbWalRecordIndex *index,
						   TupleDesc tupdesc,
						   FbReplayResult *result,
						   FbReverseOpSource *source)
{
	FbReplayDiscoverState *discover = NULL;
	FbReplayWarmState *warm = NULL;

	discover = fb_replay_discover(info, index);
	warm = fb_replay_warm(info, index, discover, result);
	fb_replay_final_build_reverse_source(info, index, tupdesc, warm, result, source);
	if (discover != NULL)
	{
		result->precomputed_missing_blocks = discover->precomputed_missing_blocks;
		result->discover_rounds = discover->discover_rounds;
		result->discover_skipped = discover->discover_skipped ? 1 : 0;
		result->summary_anchor_hits = discover->summary_anchor_hits;
		result->summary_anchor_fallback = discover->summary_anchor_fallback;
		result->summary_anchor_segments_read =
			discover->summary_anchor_segments_read;
	}
	fb_replay_warm_destroy(warm);
	fb_replay_discover_destroy(discover);
}

Datum
fb_replay_apply_image_contract_debug(PG_FUNCTION_ARGS)
{
	FbReplayStore store;
	FbReplayResult result;
	FbReplayBlockState *state;
	FbRecordRef record;
	FbRecordBlockRef *block_ref;
	char existing_page[BLCKSZ];
	char image_page[BLCKSZ];
	bool found = false;
	bool ready = false;
	bool preserve_existing;
	bool materialize_requires_apply;

	MemSet(&store, 0, sizeof(store));
	MemSet(&result, 0, sizeof(result));
	MemSet(&record, 0, sizeof(record));

	store.blocks = fb_replay_create_block_hash();
	record.kind = FB_WAL_RECORD_XLOG_FPI;
	record.block_count = 1;
	block_ref = &record.blocks[0];
	block_ref->in_use = true;
	block_ref->forknum = MAIN_FORKNUM;
	block_ref->blkno = 42;
	block_ref->has_image = true;
	block_ref->apply_image = false;
	block_ref->image = image_page;

	fb_replay_debug_seed_page((Page) existing_page, block_ref->blkno, 3);
	fb_replay_debug_seed_page((Page) image_page, block_ref->blkno, 1);

	state = fb_replay_get_block(&store, block_ref, &result, &found);
	if (found)
		elog(ERROR, "unexpected pre-existing debug replay block");
	MemSet(state, 0, sizeof(*state));
	state->key.locator = block_ref->locator;
	state->key.forknum = block_ref->forknum;
	state->key.blkno = block_ref->blkno;
	memcpy(state->page, existing_page, BLCKSZ);
	state->initialized = true;

	fb_replay_ensure_block_ready(&store, block_ref, &record, false,
								 NULL, &result, &state, &ready);
	preserve_existing = ready &&
		PageGetMaxOffsetNumber((Page) state->page) == 3;

	materialize_requires_apply =
		!fb_wal_record_block_materializes_page(&record, 0);
	block_ref->apply_image = true;
	materialize_requires_apply =
		materialize_requires_apply &&
		fb_wal_record_block_materializes_page(&record, 0);

	hash_destroy(store.blocks);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"preserve_existing=%s materialize_requires_apply=%s",
		preserve_existing ? "true" : "false",
		materialize_requires_apply ? "true" : "false")));
}

Datum
fb_replay_nonapply_image_missing_contract_debug(PG_FUNCTION_ARGS)
{
	FbReplayStore store;
	FbReplayResult result;
	FbReplayControl control;
	FbRecordRef record;
	FbRecordBlockRef *block_ref;
	FbReplayBlockState *state = NULL;
	char image_page[BLCKSZ];
	bool ready = false;
	bool found = false;
	bool notes_missing;

	MemSet(&store, 0, sizeof(store));
	MemSet(&result, 0, sizeof(result));
	MemSet(&control, 0, sizeof(control));
	MemSet(&record, 0, sizeof(record));
	MemSet(image_page, 0, sizeof(image_page));

	store.blocks = fb_replay_create_block_hash();
	control.phase = FB_REPLAY_PHASE_DISCOVER;
	control.missing_blocks = fb_replay_create_missing_block_hash();
	control.record_index = 7;

	record.kind = FB_WAL_RECORD_XLOG_FPI_FOR_HINT;
	record.lsn = InvalidXLogRecPtr + 1;
	record.end_lsn = record.lsn + 1;
	record.block_count = 1;

	block_ref = &record.blocks[0];
	block_ref->in_use = true;
	block_ref->forknum = MAIN_FORKNUM;
	block_ref->blkno = 42;
	block_ref->has_image = true;
	block_ref->apply_image = false;
	block_ref->image = image_page;

	fb_replay_ensure_block_ready(&store, block_ref, &record, false,
								 &control, &result, &state, &ready);
	notes_missing =
		fb_replay_missing_block_count(control.missing_blocks) == 1;
	state = fb_replay_find_existing_block(&store, block_ref);
	if (state != NULL)
		found = state->initialized;

	hash_destroy(control.missing_blocks);
	hash_destroy(store.blocks);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"ready=%s notes_missing=%s initialized=%s anchor_requires_apply=%s",
		ready ? "true" : "false",
		notes_missing ? "true" : "false",
		found ? "true" : "false",
		fb_replay_record_can_anchor_block(&record, 0) ? "false" : "true")));
}

Datum
fb_replay_heap_update_same_block_init_contract_debug(PG_FUNCTION_ARGS)
{
	FbReplayStore store;
	FbReplayResult result;
	FbReplayBlockState *state;
	FbRecordRef record;
	FbRecordBlockRef *block_ref;
	xl_heap_update xlrec;
	bool ready = false;
	bool allow_init;
	bool initialized;

	MemSet(&store, 0, sizeof(store));
	MemSet(&result, 0, sizeof(result));
	MemSet(&record, 0, sizeof(record));
	MemSet(&xlrec, 0, sizeof(xlrec));

	store.blocks = fb_replay_create_block_hash();
	record.kind = FB_WAL_RECORD_HEAP_UPDATE;
	record.init_page = true;
	record.block_count = 1;
	record.main_data = (char *) &xlrec;
	record.main_data_len = SizeOfHeapUpdate;

	block_ref = &record.blocks[0];
	block_ref->in_use = true;
	block_ref->block_id = 0;
	block_ref->forknum = MAIN_FORKNUM;
	block_ref->blkno = 42;

	allow_init = fb_record_allows_init_for_block(&record, 0);
	fb_replay_ensure_block_ready(&store,
								 block_ref,
								 &record,
								 allow_init,
								 NULL,
								 &result,
								 &state,
								 &ready);
	initialized = ready && state != NULL && state->initialized;

	hash_destroy(store.blocks);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"same_block_update_allow_init=%s same_block_update_ready=%s",
		allow_init ? "true" : "false",
		initialized ? "true" : "false")));
}

Datum
fb_replay_heap_update_block_id_contract_debug(PG_FUNCTION_ARGS)
{
	FbReplayStore store;
	FbReplayResult result;
	FbRecordRef record;
	FbReplayBlockState *old_state;
	FbReplayBlockState *new_state;
	FbRecordBlockRef *old_block_ref;
	FbRecordBlockRef *new_block_ref;
	xl_heap_update xlrec;
	xl_heap_header xlhdr;
	char update_data[SizeOfHeapHeader + 2];
	char old_page[BLCKSZ];
	char new_page[BLCKSZ];
	bool found = false;
	bool old_target_ok;
	bool new_target_ok;
	uint32 old_maxoff;
	uint32 new_maxoff;
	const char *old_lp3;
	const char *new_lp3;

	MemSet(&store, 0, sizeof(store));
	MemSet(&result, 0, sizeof(result));
	MemSet(&record, 0, sizeof(record));
	MemSet(&xlrec, 0, sizeof(xlrec));
	MemSet(&xlhdr, 0, sizeof(xlhdr));
	MemSet(update_data, 0, sizeof(update_data));

	store.blocks = fb_replay_create_block_hash();
	record.kind = FB_WAL_RECORD_HEAP_UPDATE;
	record.block_count = 2;
	record.main_data = (char *) &xlrec;
	record.main_data_len = SizeOfHeapUpdate;

	old_block_ref = &record.blocks[0];
	old_block_ref->in_use = true;
	old_block_ref->block_id = 1;
	old_block_ref->forknum = MAIN_FORKNUM;
	old_block_ref->blkno = 41;

	new_block_ref = &record.blocks[1];
	new_block_ref->in_use = true;
	new_block_ref->block_id = 0;
	new_block_ref->forknum = MAIN_FORKNUM;
	new_block_ref->blkno = 42;
	new_block_ref->has_data = true;
	new_block_ref->data = update_data;
	new_block_ref->data_len = sizeof(update_data);

	xlrec.old_offnum = 2;
	xlrec.new_offnum = 3;
	xlrec.old_xmax = FirstNormalTransactionId;
	xlrec.new_xmax = InvalidTransactionId;

	xlhdr.t_infomask2 = 1;
	xlhdr.t_infomask = HEAP_HASVARWIDTH;
	xlhdr.t_hoff = SizeofHeapTupleHeader;
	memcpy(update_data, &xlhdr, SizeOfHeapHeader);
	memcpy(update_data + SizeOfHeapHeader, "x", 2);

	fb_replay_debug_seed_page((Page) old_page, old_block_ref->blkno, 2);
	fb_replay_debug_seed_page((Page) new_page, new_block_ref->blkno, 2);

	old_state = fb_replay_get_block(&store, old_block_ref, &result, &found);
	if (found)
		elog(ERROR, "unexpected pre-existing old debug replay block");
	MemSet(old_state, 0, sizeof(*old_state));
	old_state->key.locator = old_block_ref->locator;
	old_state->key.forknum = old_block_ref->forknum;
	old_state->key.blkno = old_block_ref->blkno;
	memcpy(old_state->page, old_page, BLCKSZ);
	old_state->initialized = true;

	new_state = fb_replay_get_block(&store, new_block_ref, &result, &found);
	if (found)
		elog(ERROR, "unexpected pre-existing new debug replay block");
	MemSet(new_state, 0, sizeof(*new_state));
	new_state->key.locator = new_block_ref->locator;
	new_state->key.forknum = new_block_ref->forknum;
	new_state->key.blkno = new_block_ref->blkno;
	memcpy(new_state->page, new_page, BLCKSZ);
	new_state->initialized = true;

	fb_replay_heap_update(NULL,
						  &record,
						  &store,
						  false,
						  NULL,
						  NULL,
						  NULL,
						  NULL,
						  &result,
						  NULL,
						  NULL);

	old_maxoff = (uint32) PageGetMaxOffsetNumber((Page) old_state->page);
	new_maxoff = (uint32) PageGetMaxOffsetNumber((Page) new_state->page);
	old_lp3 = fb_replay_debug_lp_state((Page) old_state->page, 3);
	new_lp3 = fb_replay_debug_lp_state((Page) new_state->page, 3);
	old_target_ok = (old_maxoff == 2 && strcmp(old_lp3, "out-of-range") == 0);
	new_target_ok = (new_maxoff == 3 && strcmp(new_lp3, "normal") == 0);

	hash_destroy(store.blocks);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"heap_update_block_id_contract=%s old_maxoff=%u new_maxoff=%u new_lp3=%s old_lp3=%s",
		(old_target_ok && new_target_ok) ? "true" : "false",
		(unsigned int) old_maxoff,
		(unsigned int) new_maxoff,
		new_lp3,
		old_lp3)));
}

Datum
fb_replay_prune_image_short_circuit_debug(PG_FUNCTION_ARGS)
{
	FbReplayStore store;
	FbReplayResult result;
	FbReplayBlockState *state;
	FbRecordRef record;
	FbRecordBlockRef *block_ref;
	xl_heap_prune xlrec;
	char existing_page[BLCKSZ];
	char image_page[BLCKSZ];
	char prune_data[offsetof(xlhp_prune_items, data) + sizeof(OffsetNumber)];
	xlhp_prune_items *dead_items = (xlhp_prune_items *) prune_data;
	bool found = false;
	bool lp1_dead;
	bool lp2_dead;
	bool lp3_normal;
	bool maxoff_matches;

	MemSet(&store, 0, sizeof(store));
	MemSet(&result, 0, sizeof(result));
	MemSet(&record, 0, sizeof(record));
	MemSet(&xlrec, 0, sizeof(xlrec));

	store.blocks = fb_replay_create_block_hash();
	record.kind = FB_WAL_RECORD_HEAP2_PRUNE;
	record.block_count = 1;
	record.main_data = (char *) &xlrec;
	record.main_data_len = SizeOfHeapPrune;
	record.end_lsn = InvalidXLogRecPtr + 1;

	block_ref = &record.blocks[0];
	block_ref->in_use = true;
	block_ref->forknum = MAIN_FORKNUM;
	block_ref->blkno = 42;
	block_ref->has_image = true;
	block_ref->apply_image = true;
	block_ref->image = image_page;
	block_ref->has_data = true;
	block_ref->data = prune_data;
	block_ref->data_len = sizeof(prune_data);

	fb_replay_debug_seed_page((Page) existing_page, block_ref->blkno, 5);
	fb_replay_debug_seed_page((Page) image_page, block_ref->blkno, 3);
	ItemIdSetDead(PageGetItemId((Page) image_page, 1));
	ItemIdSetDead(PageGetItemId((Page) image_page, 2));

	xlrec.flags = XLHP_HAS_DEAD_ITEMS | XLHP_CLEANUP_LOCK;
	dead_items->ntargets = 1;
	dead_items->data[0] = 3;

	state = fb_replay_get_block(&store, block_ref, &result, &found);
	if (found)
		elog(ERROR, "unexpected pre-existing debug replay block");
	MemSet(state, 0, sizeof(*state));
	state->key.locator = block_ref->locator;
	state->key.forknum = block_ref->forknum;
	state->key.blkno = block_ref->blkno;
	memcpy(state->page, existing_page, BLCKSZ);
	state->initialized = true;

	fb_replay_heap2_prune(&record, &store, NULL, &result);

	state = fb_replay_find_existing_block(&store, block_ref);
	if (state == NULL || !state->initialized)
		elog(ERROR, "debug replay prune did not materialize block");

	lp1_dead = ItemIdIsDead(PageGetItemId((Page) state->page, 1));
	lp2_dead = ItemIdIsDead(PageGetItemId((Page) state->page, 2));
	lp3_normal = ItemIdIsNormal(PageGetItemId((Page) state->page, 3));
	maxoff_matches = (PageGetMaxOffsetNumber((Page) state->page) == 3);

	hash_destroy(store.blocks);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"prune_image_short_circuit=%s",
		(lp1_dead && lp2_dead && lp3_normal && maxoff_matches) ? "true" : "false")));
}

Datum
fb_replay_prune_image_preserve_next_insert_debug(PG_FUNCTION_ARGS)
{
	FbReplayBlockKey key;
	FbRecordRef next_record;
	char current_page[BLCKSZ];
	char image_page[BLCKSZ];
	xl_heap_insert xlrec;
	bool preserve;

	MemSet(&key, 0, sizeof(key));
	MemSet(&next_record, 0, sizeof(next_record));
	MemSet(&xlrec, 0, sizeof(xlrec));

	key.forknum = MAIN_FORKNUM;
	key.blkno = 42;

	fb_replay_debug_seed_page((Page) current_page, key.blkno, 3);
	fb_replay_debug_seed_page((Page) image_page, key.blkno, 1);

	next_record.kind = FB_WAL_RECORD_HEAP_INSERT;
	next_record.block_count = 1;
	next_record.main_data = (char *) &xlrec;
	next_record.main_data_len = SizeOfHeapInsert;
	next_record.blocks[0].in_use = true;
	next_record.blocks[0].locator = key.locator;
	next_record.blocks[0].forknum = key.forknum;
	next_record.blocks[0].blkno = key.blkno;
	xlrec.offnum = 4;

	preserve = fb_replay_prune_image_should_preserve_for_next_record(&key,
																 (Page) current_page,
																 (Page) image_page,
																 &next_record);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"prune_image_preserve_next_insert=%s",
		preserve ? "true" : "false")));
}

Datum
fb_replay_prune_image_preserve_next_multi_insert_debug(PG_FUNCTION_ARGS)
{
	struct
	{
		xl_heap_multi_insert header;
		OffsetNumber offsets[1];
	} xlwrap;
	FbReplayBlockKey key;
	FbRecordRef next_record;
	char current_page[BLCKSZ];
	char image_page[BLCKSZ];
	bool preserve;

	MemSet(&key, 0, sizeof(key));
	MemSet(&next_record, 0, sizeof(next_record));
	MemSet(&xlwrap, 0, sizeof(xlwrap));

	key.forknum = MAIN_FORKNUM;
	key.blkno = 42;

	fb_replay_debug_seed_page((Page) current_page, key.blkno, 4);
	fb_replay_debug_seed_page((Page) image_page, key.blkno, 4);
	ItemIdSetUnused(PageGetItemId((Page) current_page, 3));

	next_record.kind = FB_WAL_RECORD_HEAP2_MULTI_INSERT;
	next_record.block_count = 1;
	next_record.main_data = (char *) &xlwrap.header;
	next_record.main_data_len = SizeOfHeapMultiInsert + sizeof(OffsetNumber);
	next_record.blocks[0].in_use = true;
	next_record.blocks[0].locator = key.locator;
	next_record.blocks[0].forknum = key.forknum;
	next_record.blocks[0].blkno = key.blkno;
	xlwrap.header.ntuples = 1;
	xlwrap.offsets[0] = 3;

	preserve = fb_replay_prune_image_should_preserve_for_next_record(&key,
																	 (Page) current_page,
																 (Page) image_page,
																 &next_record);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"prune_image_preserve_next_multi_insert=%s",
		preserve ? "true" : "false")));
}

Datum
fb_replay_prune_image_preserve_dead_old_tuple_debug(PG_FUNCTION_ARGS)
{
	FbReplayBlockKey key;
	FbRecordRef next_record;
	char current_page[BLCKSZ];
	char image_page[BLCKSZ];
	xl_heap_lock xlrec;
	bool preserve;

	MemSet(&key, 0, sizeof(key));
	MemSet(&next_record, 0, sizeof(next_record));
	MemSet(&xlrec, 0, sizeof(xlrec));

	key.forknum = MAIN_FORKNUM;
	key.blkno = 42;

	fb_replay_debug_seed_page((Page) current_page, key.blkno, 4);
	fb_replay_debug_seed_page((Page) image_page, key.blkno, 1);
	ItemIdMarkDead(PageGetItemId((Page) current_page, 1));
	ItemIdSetUnused(PageGetItemId((Page) image_page, 1));

	next_record.kind = FB_WAL_RECORD_HEAP_LOCK;
	next_record.block_count = 1;
	next_record.main_data = (char *) &xlrec;
	next_record.main_data_len = SizeOfHeapLock;
	next_record.blocks[0].in_use = true;
	next_record.blocks[0].locator = key.locator;
	next_record.blocks[0].forknum = key.forknum;
	next_record.blocks[0].blkno = key.blkno;
	xlrec.offnum = 1;

	preserve = fb_replay_prune_image_should_preserve_for_next_record(&key,
																 (Page) current_page,
																 (Page) image_page,
																 &next_record);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"prune_image_preserve_dead_old_tuple=%s",
		preserve ? "true" : "false")));
}

Datum
fb_replay_prune_image_reject_used_insert_slot_debug(PG_FUNCTION_ARGS)
{
	FbReplayBlockKey key;
	FbRecordRef next_record;
	char current_page[BLCKSZ];
	char image_page[BLCKSZ];
	xl_heap_insert xlrec;
	bool preserve;

	MemSet(&key, 0, sizeof(key));
	MemSet(&next_record, 0, sizeof(next_record));
	MemSet(&xlrec, 0, sizeof(xlrec));

	key.forknum = MAIN_FORKNUM;
	key.blkno = 42;

	fb_replay_debug_seed_page((Page) current_page, key.blkno, 4);
	fb_replay_debug_seed_page((Page) image_page, key.blkno, 1);

	next_record.kind = FB_WAL_RECORD_HEAP_INSERT;
	next_record.block_count = 1;
	next_record.main_data = (char *) &xlrec;
	next_record.main_data_len = SizeOfHeapInsert;
	next_record.blocks[0].in_use = true;
	next_record.blocks[0].locator = key.locator;
	next_record.blocks[0].forknum = key.forknum;
	next_record.blocks[0].blkno = key.blkno;
	xlrec.offnum = 4;

	preserve = fb_replay_prune_image_should_preserve_for_next_record(&key,
																 (Page) current_page,
																 (Page) image_page,
																 &next_record);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"prune_image_reject_used_insert_slot=%s",
		(!preserve) ? "true" : "false")));
}

Datum
fb_replay_prune_image_reject_future_warm_state_debug(PG_FUNCTION_ARGS)
{
	FbReplayBlockKey key;
	FbRecordRef insert2;
	FbRecordRef insert3;
	FbRecordRef insert4;
	FbRecordRef cleanup;
	FbRecordRef record;
	FbReplayBlockState state;
	FbReplayControl control;
	FbReplayPruneLookaheadEntry *entry;
	FbReplayFutureBlockRecord future;
	char current_page[BLCKSZ];
	char image_page[BLCKSZ];
	char prune_data[offsetof(xlhp_prune_items, data) + sizeof(OffsetNumber) * 4];
	xl_heap_insert insert2_xlrec;
	xl_heap_insert insert3_xlrec;
	xl_heap_insert insert4_xlrec;
	xl_heap_prune cleanup_xlrec;
	xlhp_prune_items *unused_items = (xlhp_prune_items *) prune_data;
	bool found;
	bool preserve;

	MemSet(&key, 0, sizeof(key));
	MemSet(&insert2, 0, sizeof(insert2));
	MemSet(&insert3, 0, sizeof(insert3));
	MemSet(&insert4, 0, sizeof(insert4));
	MemSet(&cleanup, 0, sizeof(cleanup));
	MemSet(&record, 0, sizeof(record));
	MemSet(&state, 0, sizeof(state));
	MemSet(&control, 0, sizeof(control));
	MemSet(&future, 0, sizeof(future));
	MemSet(&insert2_xlrec, 0, sizeof(insert2_xlrec));
	MemSet(&insert3_xlrec, 0, sizeof(insert3_xlrec));
	MemSet(&insert4_xlrec, 0, sizeof(insert4_xlrec));
	MemSet(&cleanup_xlrec, 0, sizeof(cleanup_xlrec));
	MemSet(prune_data, 0, sizeof(prune_data));

	key.forknum = MAIN_FORKNUM;
	key.blkno = 42;

	fb_replay_debug_seed_page((Page) current_page, key.blkno, 6);
	fb_replay_debug_seed_page((Page) image_page, key.blkno, 5);
	ItemIdSetUnused(PageGetItemId((Page) current_page, 2));
	ItemIdSetUnused(PageGetItemId((Page) current_page, 3));
	ItemIdSetUnused(PageGetItemId((Page) current_page, 4));
	ItemIdMarkDead(PageGetItemId((Page) image_page, 2));
	ItemIdMarkDead(PageGetItemId((Page) image_page, 3));
	ItemIdMarkDead(PageGetItemId((Page) image_page, 4));
	ItemIdMarkDead(PageGetItemId((Page) image_page, 5));

	insert2.kind = FB_WAL_RECORD_HEAP_INSERT;
	insert2.block_count = 1;
	insert2.blocks[0].in_use = true;
	insert2.blocks[0].locator = key.locator;
	insert2.blocks[0].forknum = key.forknum;
	insert2.blocks[0].blkno = key.blkno;
	insert2.main_data = (char *) &insert2_xlrec;
	insert2.main_data_len = SizeOfHeapInsert;
	insert2_xlrec.offnum = 2;

	insert3 = insert2;
	insert3.main_data = (char *) &insert3_xlrec;
	insert3_xlrec.offnum = 3;

	insert4 = insert2;
	insert4.main_data = (char *) &insert4_xlrec;
	insert4_xlrec.offnum = 4;

	cleanup.kind = FB_WAL_RECORD_HEAP2_PRUNE;
	cleanup.block_count = 1;
	cleanup.blocks[0].in_use = true;
	cleanup.blocks[0].locator = key.locator;
	cleanup.blocks[0].forknum = key.forknum;
	cleanup.blocks[0].blkno = key.blkno;
	cleanup.blocks[0].has_data = true;
	cleanup.blocks[0].data = prune_data;
	cleanup.blocks[0].data_len = sizeof(prune_data);
	cleanup.main_data = (char *) &cleanup_xlrec;
	cleanup.main_data_len = SizeOfHeapPrune;
	cleanup_xlrec.flags = XLHP_HAS_NOW_UNUSED_ITEMS;
	unused_items->ntargets = 4;
	unused_items->data[0] = 2;
	unused_items->data[1] = 3;
	unused_items->data[2] = 4;
	unused_items->data[3] = 5;

	fb_replay_future_block_record_compose(&insert4, 0, NULL, &future);
	fb_replay_future_block_record_compose(&insert3, 0, &future, &future);
	fb_replay_future_block_record_compose(&insert2, 0, &future, &future);
	fb_replay_future_block_record_compose(&cleanup, 0, &future, &future);

	record.kind = FB_WAL_RECORD_HEAP2_PRUNE;
	record.lsn = InvalidXLogRecPtr + 32;
	record.end_lsn = InvalidXLogRecPtr + 48;
	record.block_count = 1;
	record.blocks[0].in_use = true;
	record.blocks[0].locator = key.locator;
	record.blocks[0].forknum = key.forknum;
	record.blocks[0].blkno = key.blkno;
	record.blocks[0].has_image = true;
	record.blocks[0].apply_image = true;
	record.blocks[0].image = image_page;

	state.key = key;
	state.initialized = true;
	memcpy(state.page, current_page, BLCKSZ);
	state.page_lsn = InvalidXLogRecPtr + 16;

	control.phase = FB_REPLAY_PHASE_FINAL;
	control.record_index = 7;
	control.prune_lookahead = fb_replay_create_prune_lookahead_hash();

	entry = (FbReplayPruneLookaheadEntry *) hash_search(control.prune_lookahead,
														&control.record_index,
														HASH_ENTER,
														&found);
	if (found)
		elog(ERROR, "unexpected existing prune lookahead debug entry");
	entry->record_index = control.record_index;
	entry->future = future;

	preserve = fb_replay_prune_image_should_preserve_page(&record,
															  &state,
															  &control);

	if (entry->future.old_tuple_offsets != NULL)
		bms_free(entry->future.old_tuple_offsets);
	if (entry->future.new_tuple_slot_offsets != NULL)
		bms_free(entry->future.new_tuple_slot_offsets);
	hash_destroy(control.prune_lookahead);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"prune_image_reject_future_warm_state=%s",
		(!preserve) ? "true" : "false")));
}

Datum
fb_replay_prune_compose_future_constraints_debug(PG_FUNCTION_ARGS)
{
	FbReplayBlockKey key;
	FbRecordRef insert4;
	FbRecordRef delete4;
	FbRecordRef delete3;
	FbReplayFutureBlockRecord future;
	FbReplayControl control;
	HTAB *lookahead;
	FbReplayPruneLookaheadEntry *entry;
	xl_heap_insert insert4_xlrec;
	xl_heap_delete delete4_xlrec;
	xl_heap_delete delete3_xlrec;
	OffsetNumber redirected_buf[2];
	OffsetNumber nowdead_buf[3];
	OffsetNumber nowunused_buf[1];
	int nredirected = 0;
	int ndead = 3;
	int nunused = 1;
	bool found;
	bool ok;

	MemSet(&key, 0, sizeof(key));
	MemSet(&insert4, 0, sizeof(insert4));
	MemSet(&delete4, 0, sizeof(delete4));
	MemSet(&delete3, 0, sizeof(delete3));
	MemSet(&future, 0, sizeof(future));
	MemSet(&control, 0, sizeof(control));
	MemSet(&insert4_xlrec, 0, sizeof(insert4_xlrec));
	MemSet(&delete4_xlrec, 0, sizeof(delete4_xlrec));
	MemSet(&delete3_xlrec, 0, sizeof(delete3_xlrec));

	key.forknum = MAIN_FORKNUM;
	key.blkno = 42;

	insert4.kind = FB_WAL_RECORD_HEAP_INSERT;
	insert4.block_count = 1;
	insert4.blocks[0].in_use = true;
	insert4.blocks[0].locator = key.locator;
	insert4.blocks[0].forknum = key.forknum;
	insert4.blocks[0].blkno = key.blkno;
	insert4.main_data = (char *) &insert4_xlrec;
	insert4.main_data_len = SizeOfHeapInsert;
	insert4_xlrec.offnum = 4;

	delete4.kind = FB_WAL_RECORD_HEAP_DELETE;
	delete4.block_count = 1;
	delete4.blocks[0].in_use = true;
	delete4.blocks[0].locator = key.locator;
	delete4.blocks[0].forknum = key.forknum;
	delete4.blocks[0].blkno = key.blkno;
	delete4.main_data = (char *) &delete4_xlrec;
	delete4.main_data_len = SizeOfHeapDelete;
	delete4_xlrec.offnum = 4;

	delete3.kind = FB_WAL_RECORD_HEAP_DELETE;
	delete3.block_count = 1;
	delete3.blocks[0].in_use = true;
	delete3.blocks[0].locator = key.locator;
	delete3.blocks[0].forknum = key.forknum;
	delete3.blocks[0].blkno = key.blkno;
	delete3.main_data = (char *) &delete3_xlrec;
	delete3.main_data_len = SizeOfHeapDelete;
	delete3_xlrec.offnum = 3;

	fb_replay_future_block_record_compose(&delete3, 0, NULL, &future);
	fb_replay_future_block_record_compose(&delete4, 0, &future, &future);
	fb_replay_future_block_record_compose(&insert4, 0, &future, &future);

	lookahead = fb_replay_create_prune_lookahead_hash();
	control.record_index = 1;
	entry = (FbReplayPruneLookaheadEntry *) hash_search(lookahead,
														&control.record_index,
														HASH_ENTER,
														&found);
	entry->record_index = 1;
	entry->future = future;

	control.phase = FB_REPLAY_PHASE_FINAL;
	control.prune_lookahead = lookahead;

	nowdead_buf[0] = 1;
	nowdead_buf[1] = 2;
	nowdead_buf[2] = 3;
	nowunused_buf[0] = 4;
	fb_replay_prune_apply_future_guard(&control,
									   redirected_buf,
									   &nredirected,
									   nowdead_buf,
									   &ndead,
									   nowunused_buf,
									   &nunused);

	ok = fb_replay_future_block_record_old_count(&future) == 1 &&
		fb_replay_future_block_record_first_old(&future) == 3 &&
		fb_replay_future_block_record_new_count(&future) == 1 &&
		fb_replay_future_block_record_first_new(&future) == 4 &&
		ndead == 3 &&
		nowdead_buf[0] == 1 &&
		nowdead_buf[1] == 2 &&
		nowdead_buf[2] == 3 &&
		nunused == 1 &&
		nowunused_buf[0] == 4;

	hash_destroy(lookahead);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"prune_compose_future_constraints=%s old_count=%d old_first=%u new_count=%d new_first=%u ndead=%d dead0=%u dead1=%u nunused=%d unused0=%u",
		ok ? "true" : "false",
		fb_replay_future_block_record_old_count(&future),
		fb_replay_future_block_record_first_old(&future),
		fb_replay_future_block_record_new_count(&future),
		fb_replay_future_block_record_first_new(&future),
		ndead,
		ndead > 0 ? nowdead_buf[0] : InvalidOffsetNumber,
		ndead > 1 ? nowdead_buf[1] : InvalidOffsetNumber,
		nunused,
		nunused > 0 ? nowunused_buf[0] : InvalidOffsetNumber)));
}

Datum
fb_replay_prune_lookahead_snapshot_isolation_debug(PG_FUNCTION_ARGS)
{
	FbReplayFutureBlockRecord after_delete42;
	FbReplayFutureBlockRecord after_insert42;
	FbReplayFutureBlockRecord after_delete24;
	FbReplayFutureBlockRecord after_delete23;
	FbReplayFutureBlockRecord snapshot;
	FbRecordRef delete42;
	FbRecordRef insert42;
	FbRecordRef delete24;
	FbRecordRef delete23;
	xl_heap_delete delete42_xlrec;
	xl_heap_insert insert42_xlrec;
	xl_heap_delete delete24_xlrec;
	xl_heap_delete delete23_xlrec;
	bool isolated;

	MemSet(&after_delete42, 0, sizeof(after_delete42));
	MemSet(&after_insert42, 0, sizeof(after_insert42));
	MemSet(&after_delete24, 0, sizeof(after_delete24));
	MemSet(&after_delete23, 0, sizeof(after_delete23));
	MemSet(&snapshot, 0, sizeof(snapshot));
	MemSet(&delete42, 0, sizeof(delete42));
	MemSet(&insert42, 0, sizeof(insert42));
	MemSet(&delete24, 0, sizeof(delete24));
	MemSet(&delete23, 0, sizeof(delete23));
	MemSet(&delete42_xlrec, 0, sizeof(delete42_xlrec));
	MemSet(&insert42_xlrec, 0, sizeof(insert42_xlrec));
	MemSet(&delete24_xlrec, 0, sizeof(delete24_xlrec));
	MemSet(&delete23_xlrec, 0, sizeof(delete23_xlrec));

	delete42.kind = FB_WAL_RECORD_HEAP_DELETE;
	delete42.block_count = 1;
	delete42.blocks[0].in_use = true;
	delete42.blocks[0].forknum = MAIN_FORKNUM;
	delete42.blocks[0].blkno = 17079;
	delete42.main_data = (char *) &delete42_xlrec;
	delete42.main_data_len = SizeOfHeapDelete;
	delete42_xlrec.offnum = 42;

	insert42.kind = FB_WAL_RECORD_HEAP_INSERT;
	insert42.block_count = 1;
	insert42.blocks[0].in_use = true;
	insert42.blocks[0].forknum = MAIN_FORKNUM;
	insert42.blocks[0].blkno = 17079;
	insert42.main_data = (char *) &insert42_xlrec;
	insert42.main_data_len = SizeOfHeapInsert;
	insert42_xlrec.offnum = 42;

	delete24.kind = FB_WAL_RECORD_HEAP_DELETE;
	delete24.block_count = 1;
	delete24.blocks[0].in_use = true;
	delete24.blocks[0].forknum = MAIN_FORKNUM;
	delete24.blocks[0].blkno = 17079;
	delete24.main_data = (char *) &delete24_xlrec;
	delete24.main_data_len = SizeOfHeapDelete;
	delete24_xlrec.offnum = 24;

	delete23.kind = FB_WAL_RECORD_HEAP_DELETE;
	delete23.block_count = 1;
	delete23.blocks[0].in_use = true;
	delete23.blocks[0].forknum = MAIN_FORKNUM;
	delete23.blocks[0].blkno = 17079;
	delete23.main_data = (char *) &delete23_xlrec;
	delete23.main_data_len = SizeOfHeapDelete;
	delete23_xlrec.offnum = 23;

	if (!fb_replay_future_block_record_compose(&delete42, 0, NULL, &after_delete42))
		elog(ERROR, "failed to compose delete42 future constraints");
	if (!fb_replay_future_block_record_compose(&insert42, 0,
												 &after_delete42,
												 &after_insert42))
		elog(ERROR, "failed to compose insert42 future constraints");
	if (!fb_replay_future_block_record_compose(&delete24, 0,
												 &after_insert42,
												 &after_delete24))
		elog(ERROR, "failed to compose delete24 future constraints");

	fb_replay_future_block_record_clone(&snapshot, &after_delete24);

	if (!fb_replay_future_block_record_compose(&delete23, 0,
												 &after_delete24,
												 &after_delete23))
		elog(ERROR, "failed to compose delete23 future constraints");

	isolated =
		fb_replay_future_block_record_old_count(&snapshot) == 1 &&
		fb_replay_future_block_record_first_old(&snapshot) == 24 &&
		fb_replay_future_block_record_new_count(&snapshot) == 1 &&
		fb_replay_future_block_record_first_new(&snapshot) == 42;

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"prune_lookahead_snapshot_isolated=%s old_count=%d old_first=%u new_count=%d new_first=%u",
		isolated ? "true" : "false",
		fb_replay_future_block_record_old_count(&snapshot),
		fb_replay_future_block_record_first_old(&snapshot),
		fb_replay_future_block_record_new_count(&snapshot),
		fb_replay_future_block_record_first_new(&snapshot))));
}

/*
 * fb_replay_execute
 *    Replay entry point.
 */

void
fb_replay_execute(const FbRelationInfo *info,
				  const FbWalRecordIndex *index,
				  FbReplayResult *result)
{
	fb_replay_execute_internal(info, index, NULL, result, NULL);
}

/*
 * fb_replay_build_reverse_source
 *    Replay entry point.
 */

void
fb_replay_build_reverse_source(const FbRelationInfo *info,
							   const FbWalRecordIndex *index,
							   TupleDesc tupdesc,
							   FbReplayResult *result,
							   FbReverseOpSource *source)
{
	fb_replay_execute_internal(info, index, tupdesc, result, source);
}
