/*
 * fb_replay.c
 *    Page replay and forward-op extraction.
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/relation.h"
#include "access/heapam_xlog.h"
#include "storage/bufpage.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "fb_memory.h"
#include "fb_progress.h"
#include "fb_replay.h"
#include "fb_toast.h"

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
	uint32 anchor_record_index;
	bool anchor_found;
} FbReplayMissingBlock;

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
} FbReplayControl;

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
fb_copy_page_tuple(Page page, BlockNumber blkno, OffsetNumber offnum)
{
	ItemId lp;
	HeapTupleData local_tuple;

	if (PageGetMaxOffsetNumber(page) < offnum)
		return NULL;

	lp = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(lp))
		return NULL;

	MemSet(&local_tuple, 0, sizeof(local_tuple));
	local_tuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);
	local_tuple.t_len = ItemIdGetLength(lp);
	ItemPointerSet(&local_tuple.t_self, blkno, offnum);

	return heap_copytuple(&local_tuple);
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
	}
	else if (control->record_index < entry->first_record_index)
		entry->first_record_index = control->record_index;
}

/*
 * fb_record_allows_init_for_block
 *    Replay helper.
 */

static bool
fb_record_allows_init_for_block(const FbRecordRef *record, int block_index)
{
	if (record == NULL || !record->init_page || block_index != 0)
		return false;

	switch (record->kind)
	{
		case FB_WAL_RECORD_HEAP_INSERT:
		case FB_WAL_RECORD_HEAP_UPDATE:
		case FB_WAL_RECORD_HEAP_HOT_UPDATE:
		case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
			return true;
		default:
			return false;
	}
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
			if (!dest_entry->anchor_found ||
				entry->anchor_record_index < dest_entry->anchor_record_index)
			{
				dest_entry->anchor_record_index = entry->anchor_record_index;
				dest_entry->anchor_found = entry->anchor_found;
			}
			if (entry->first_record_index < dest_entry->first_record_index)
				dest_entry->first_record_index = entry->first_record_index;
		}
	}
}

/*
 * fb_replay_resolve_missing_anchors
 *    Replay helper.
 */

static void
fb_replay_resolve_missing_anchors(const FbWalRecordIndex *index,
								  HTAB *missing_blocks)
{
	uint32 unresolved;
	FbWalRecordCursor *cursor;
	FbRecordRef record;
	uint32 record_index;

	unresolved = fb_replay_missing_block_count(missing_blocks);
	if (unresolved == 0)
		return;

	cursor = fb_wal_record_cursor_open(index, FB_SPOOL_BACKWARD);
	while (unresolved > 0 &&
		   fb_wal_record_cursor_read(cursor, &record, &record_index))
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

			if (!block_ref->has_image &&
				!fb_record_allows_init_for_block(&record, block_index))
				continue;

			entry->anchor_found = true;
			entry->anchor_record_index = record_index;
			unresolved--;
		}
	}
	fb_wal_record_cursor_close(cursor);
}

/*
 * fb_replay_min_anchor_index
 *    Replay helper.
 */

static uint32
fb_replay_min_anchor_index(HTAB *missing_blocks)
{
	HASH_SEQ_STATUS seq;
	FbReplayMissingBlock *entry;
	uint32 min_index = UINT32_MAX;

	hash_seq_init(&seq, missing_blocks);
	while ((entry = (FbReplayMissingBlock *) hash_seq_search(&seq)) != NULL)
	{
		if (!entry->anchor_found)
			continue;
		if (entry->anchor_record_index < min_index)
			min_index = entry->anchor_record_index;
	}

	return min_index;
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
									 HTAB *missing_blocks,
									 uint32 record_index)
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
		if (!entry->anchor_found || record_index < entry->anchor_record_index)
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

	if (block_ref->has_image && block_ref->image != NULL)
	{
		memcpy(state->page, block_ref->image, BLCKSZ);
		state->initialized = true;
		result->blocks_materialized++;
	}
	else if (!state->initialized && allow_init)
	{
		PageInit((Page) state->page, BLCKSZ, 0);
		state->initialized = true;
		result->blocks_materialized++;
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
		!block_ref->apply_image ||
		toast_store == NULL ||
		toast_tupdesc == NULL)
		return;

	fb_toast_store_sync_page(toast_store, toast_tupdesc, page, block_ref->blkno);
}

/*
 * fb_replay_get_tuple_from_page
 *    Replay helper.
 */

static HeapTupleHeader
fb_replay_get_tuple_from_page(Page page, OffsetNumber offnum, uint32 *tuple_len)
{
	ItemId lp;

	if (PageGetMaxOffsetNumber(page) < offnum)
		return NULL;

	lp = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(lp))
		return NULL;

	if (tuple_len != NULL)
		*tuple_len = ItemIdGetLength(lp);
	return (HeapTupleHeader) PageGetItem(page, lp);
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

	fb_replay_ensure_block_ready(store, block_ref, record, record->init_page,
								 control, result, &state, &ready);
	if (!ready)
		return;
	page = (Page) state->page;
	image_applied = block_ref->has_image && block_ref->apply_image;

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

		if (PageAddItem(page, (Item) htup, newlen, xlrec->offnum, true, true) == InvalidOffsetNumber)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("failed to replay heap insert"),
					 errdetail("lsn=%X/%08X rel=%u/%u/%u blk=%u off=%u image=%s apply_image=%s data=%s init_page=%s maxoff=%u has_item=%s",
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
								ItemIdIsUsed(PageGetItemId(page, xlrec->offnum))) ? "true" : "false")));
	}

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
	bool ready;

	fb_replay_ensure_block_ready(store, block_ref, record, false,
								 control, result, &state, &ready);
	if (!ready)
		return;
	page = (Page) state->page;
	fb_replay_sync_toast_page_if_image(block_ref, page, toast_tupdesc, toast_store);

	if (source != NULL && tupdesc != NULL &&
		record->committed_after_target && block_ref->is_main_relation)
		old_tuple = fb_copy_page_tuple(page, block_ref->blkno, xlrec->offnum);
	if (block_ref->is_toast_relation && toast_store != NULL && toast_tupdesc != NULL)
		old_toast_tuple = fb_copy_page_tuple(page, block_ref->blkno, xlrec->offnum);

	htup = fb_replay_get_tuple_from_page(page, xlrec->offnum, NULL);
	if (htup == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("failed to locate tuple for heap delete redo")));

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

	state->page_lsn = record->end_lsn;
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
	FbRecordBlockRef *new_block_ref = (FbRecordBlockRef *) &record->blocks[0];
	FbRecordBlockRef *old_block_ref = (record->block_count > 1) ?
		(FbRecordBlockRef *) &record->blocks[1] : (FbRecordBlockRef *) &record->blocks[0];
	FbReplayBlockState *old_state;
	FbReplayBlockState *new_state;
	FbRecordBlockRef *tuple_data_ref;
	bool new_page_image_only;
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

	fb_replay_ensure_block_ready(store, old_block_ref, record, false,
								 control, result, &old_state, &old_ready);
	if (!old_ready)
		return;
	if (old_block_ref == new_block_ref)
	{
		new_state = old_state;
		new_ready = true;
	}
	else
		fb_replay_ensure_block_ready(store, new_block_ref, record, record->init_page,
									 control, result, &new_state, &new_ready);
	if (!new_ready)
		return;

	oldpage = (Page) old_state->page;
	newpage = (Page) new_state->page;
	new_page_image_only = (new_block_ref->has_image &&
						   old_block_ref != new_block_ref);
	fb_replay_sync_toast_page_if_image(old_block_ref, oldpage, toast_tupdesc, toast_store);
	if (new_block_ref != old_block_ref)
		fb_replay_sync_toast_page_if_image(new_block_ref, newpage, toast_tupdesc, toast_store);

	if (source != NULL && tupdesc != NULL &&
		record->committed_after_target && old_block_ref->is_main_relation)
		old_tuple = fb_copy_page_tuple(oldpage, old_block_ref->blkno, xlrec->old_offnum);
	if (old_block_ref->is_toast_relation && toast_store != NULL && toast_tupdesc != NULL)
		old_toast_tuple = fb_copy_page_tuple(oldpage, old_block_ref->blkno, xlrec->old_offnum);

	offnum = xlrec->old_offnum;
	oldtup = fb_replay_get_tuple_from_page(oldpage, offnum, &oldtup_len);
	if (oldtup == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("failed to locate tuple for heap update redo")));

	if (PageGetMaxOffsetNumber(oldpage) < offnum)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid old offnum during heap update redo")));

	lp = PageGetItemId(oldpage, offnum);
	htup = (HeapTupleHeader) PageGetItem(oldpage, lp);

	ItemPointerSet(&newtid, new_block_ref->blkno, xlrec->new_offnum);

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

	if (!new_page_image_only)
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

		if (PageGetMaxOffsetNumber(newpage) + 1 < offnum)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("invalid new offnum during heap update redo")));

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
					 errmsg("failed to replay heap update")));

		if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(newpage);
	}

	old_state->page_lsn = record->end_lsn;
	new_state->page_lsn = record->end_lsn;
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
	}
	if (new_block_ref->is_toast_relation && toast_store != NULL && toast_tupdesc != NULL)
	{
		if (old_toast_tuple != NULL)
			fb_toast_store_remove_from_tuple(toast_store, toast_tupdesc, old_toast_tuple);
		if (new_page_image_only)
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
	bool ready;

	if (!block_ref->has_image && !record->init_page)
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
	if (block_ref->has_image || record->init_page)
	{
		if (!ready)
			return;
	}

	page = (Page) state->page;
	htup = fb_replay_get_tuple_from_page(page, xlrec->offnum, NULL);
	if (htup == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("failed to locate tuple for heap lock redo")));

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
	bool ready;

	if (!block_ref->has_image && !record->init_page)
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
	if (block_ref->has_image || record->init_page)
		{
			if (!ready)
				return;
		}

	page = (Page) state->page;
	if (record->main_data != NULL && record->main_data_len >= SizeOfHeapPrune)
		memcpy(&xlrec, record->main_data, SizeOfHeapPrune);
	else
		MemSet(&xlrec, 0, sizeof(xlrec));

	if (block_ref->has_data && block_ref->data_len > 0)
	{
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

	state->page_lsn = record->end_lsn;
	result->records_replayed++;
#else
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	bool ready;

	if (!block_ref->has_image && !record->init_page)
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
	if (block_ref->has_image || record->init_page)
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
	bool ready;

	fb_replay_ensure_block_ready(store, block_ref, record, false,
								 control, result, &state, &ready);
	if (!ready)
		return;
	page = (Page) state->page;
	htup = fb_replay_get_tuple_from_page(page, xlrec->offnum, NULL);
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
	bool ready;

	fb_replay_ensure_block_ready(store, block_ref, record, false,
								 control, result, &state, &ready);
	if (!ready)
		return;
	page = (Page) state->page;
	htup = fb_replay_get_tuple_from_page(page, xlrec->offnum, NULL);
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
	bool ready;

	fb_replay_ensure_block_ready(store, block_ref, record, false,
								 control, result, &state, &ready);
	if (!ready)
		return;
	page = (Page) state->page;
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
	bool ready;

	if (!block_ref->has_image && !record->init_page)
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
	if (block_ref->has_image || record->init_page)
	{
		if (!ready)
			return;
	}

	page = (Page) state->page;
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
	image_applied = block_ref->has_image && block_ref->apply_image;

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

			if (PageAddItem(page, (Item) htup, newlen, offnum, true, true) == InvalidOffsetNumber)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("failed to replay heap multi insert")));
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
				   XLogRecPtr lower_bound,
				   XLogRecPtr upper_bound)
{
	FbWalRecordCursor *cursor;
	FbRecordRef record;
	uint32 record_index;
	uint32 progress_stride = 1;

	if (index != NULL && index->record_count > 0)
		progress_stride = Max((uint32) 1, index->record_count / 1024);

	cursor = fb_wal_record_cursor_open(index, FB_SPOOL_FORWARD);
	while (fb_wal_record_cursor_read(cursor, &record, &record_index))
	{
		if (!XLogRecPtrIsInvalid(lower_bound) && record.lsn < lower_bound)
			continue;
		if (!XLogRecPtrIsInvalid(upper_bound) && record.lsn >= upper_bound)
			continue;
		if (record.aborted)
			continue;
		if (control != NULL)
		{
			control->record_index = record_index;
			if (record_index == 0 ||
				(record_index + 1) >= index->record_count ||
				((record_index + 1) % progress_stride) == 0)
				fb_replay_progress_update(control,
										 (uint64) record_index + 1,
										 index->record_count);
		}
		if (control != NULL &&
			control->phase == FB_REPLAY_PHASE_WARM &&
			!fb_record_should_apply_for_backtrack(&record,
												 control->missing_blocks,
												 record_index))
			continue;

		switch (record.kind)
		{
			case FB_WAL_RECORD_HEAP_INSERT:
				fb_replay_heap_insert(info, &record, store, tupdesc, toast_tupdesc,
									  toast_store, control, result, source,
									  pending_ops);
				break;
			case FB_WAL_RECORD_HEAP_DELETE:
				fb_replay_heap_delete(info, &record, store, tupdesc, toast_tupdesc,
									  toast_store, control, result, source,
									  pending_ops);
				break;
			case FB_WAL_RECORD_HEAP_UPDATE:
				fb_replay_heap_update(info, &record, store, false, tupdesc, toast_tupdesc,
									  toast_store, control, result, source,
									  pending_ops);
				break;
			case FB_WAL_RECORD_HEAP_HOT_UPDATE:
				fb_replay_heap_update(info, &record, store, true, tupdesc, toast_tupdesc,
									  toast_store, control, result, source,
									  pending_ops);
				break;
			case FB_WAL_RECORD_HEAP_CONFIRM:
				fb_replay_heap_confirm(&record, store, control, result);
				break;
			case FB_WAL_RECORD_HEAP_LOCK:
				fb_replay_heap_lock(&record, store, control, result);
				break;
			case FB_WAL_RECORD_HEAP_INPLACE:
				fb_replay_heap_inplace(&record, store, control, result);
				break;
			case FB_WAL_RECORD_HEAP2_PRUNE:
				fb_replay_heap2_prune(&record, store, control, result);
				break;
			case FB_WAL_RECORD_HEAP2_VISIBLE:
				fb_replay_heap2_visible(&record, store, control, result);
				break;
			case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
				fb_replay_heap2_multi_insert(info, &record, store, tupdesc, toast_tupdesc,
											 toast_store, control, result, source,
											 pending_ops);
				break;
			case FB_WAL_RECORD_HEAP2_LOCK_UPDATED:
				fb_replay_heap2_lock_updated(&record, store, control, result);
				break;
			case FB_WAL_RECORD_XLOG_FPI:
			case FB_WAL_RECORD_XLOG_FPI_FOR_HINT:
				fb_replay_xlog_fpi(&record, store, control, result, toast_tupdesc, toast_store);
				break;
		}
	}
	fb_wal_record_cursor_close(cursor);
}

/*
 * fb_replay_warm_store
 *    Replay helper.
 */

static void
fb_replay_warm_store(const FbRelationInfo *info,
					 const FbWalRecordIndex *index,
					 TupleDesc toast_tupdesc,
					 FbToastStore *toast_store,
					 FbReplayStore *store,
					 HTAB *backtrack_blocks,
					 FbReplayResult *result)
{
	FbReplayControl warm_control;
	uint32 min_anchor_index;
	XLogRecPtr warm_start;

	if (fb_replay_missing_block_count(backtrack_blocks) == 0)
	{
		fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_WARM, 100, NULL);
		return;
	}

	min_anchor_index = fb_replay_min_anchor_index(backtrack_blocks);
	if (min_anchor_index == UINT32_MAX)
	{
		fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_WARM, 100, NULL);
		return;
	}

	{
		FbRecordRef anchor_record;

		if (!fb_wal_record_load(index, min_anchor_index, &anchor_record))
			elog(ERROR, "missing warm anchor record %u", min_anchor_index);
		warm_start = anchor_record.lsn;
	}
	MemSet(&warm_control, 0, sizeof(warm_control));
	warm_control.phase = FB_REPLAY_PHASE_WARM;
	warm_control.missing_blocks = backtrack_blocks;
	fb_replay_run_pass(info, index, NULL, toast_tupdesc, toast_store,
					   store, &warm_control, result, NULL, NULL,
					   warm_start, index->anchor_redo_lsn);
	fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_WARM, 100, NULL);
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
	FbReplayStore store;
	FbToastStore *toast_store = NULL;
	FbToastStore *discover_toast_store = NULL;
	Relation toast_rel = NULL;
	TupleDesc toast_tupdesc = NULL;
	HTAB *backtrack_blocks;
	uint32 iteration;
	uint64 warm_tracked_bytes;

	if (info->has_toast_locator && OidIsValid(info->toast_relid))
	{
		toast_rel = relation_open(info->toast_relid, AccessShareLock);
		toast_tupdesc = CreateTupleDescCopy(toast_rel->rd_att);
		relation_close(toast_rel, AccessShareLock);
	}

	backtrack_blocks = fb_replay_create_missing_block_hash();

	for (iteration = 0; iteration < 8; iteration++)
	{
		MemoryContext discover_ctx;
		MemoryContext oldctx;
		FbReplayStore discover_store;
		FbReplayControl discover_control;
		FbReplayResult discover_result;
		HTAB *iteration_missing;

		discover_ctx = AllocSetContextCreate(CurrentMemoryContext,
											 "fb replay discover",
											 ALLOCSET_DEFAULT_SIZES);
		oldctx = MemoryContextSwitchTo(discover_ctx);
		MemSet(&discover_store, 0, sizeof(discover_store));
		discover_store.blocks = fb_replay_create_block_hash();
		if (toast_tupdesc != NULL)
			discover_toast_store = fb_toast_store_create();
		fb_replay_init_result(index, &discover_result);
		MemoryContextSwitchTo(oldctx);

		fb_replay_warm_store(info, index, toast_tupdesc, discover_toast_store,
							 &discover_store, backtrack_blocks, &discover_result);

		iteration_missing = fb_replay_create_missing_block_hash();
		MemSet(&discover_control, 0, sizeof(discover_control));
		discover_control.phase = FB_REPLAY_PHASE_DISCOVER;
		discover_control.round_no = iteration + 1;
		discover_control.missing_blocks = iteration_missing;
		fb_replay_progress_enter(&discover_control);
		fb_replay_run_pass(info, index, NULL, toast_tupdesc, discover_toast_store,
						   &discover_store, &discover_control, &discover_result,
						   NULL, NULL, index->anchor_redo_lsn, InvalidXLogRecPtr);
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
				discover_toast_store = NULL;
				MemoryContextDelete(discover_ctx);
				break;
			}

			fb_replay_resolve_missing_anchors(index, iteration_missing);
			fb_replay_raise_unresolved_missing_fpi(index, iteration_missing);
			fb_replay_merge_missing_blocks(backtrack_blocks, iteration_missing);
			hash_destroy(iteration_missing);
			if (discover_toast_store != NULL)
				fb_toast_store_destroy(discover_toast_store);
			discover_toast_store = NULL;
			MemoryContextDelete(discover_ctx);

		if (iteration == 7)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("too many shared backtracking rounds while resolving missing FPI")));
	}

	MemSet(&store, 0, sizeof(store));
	store.blocks = fb_replay_create_block_hash();
	fb_replay_init_result(index, result);
	if (toast_tupdesc != NULL)
		toast_store = fb_toast_store_create();

	fb_progress_enter_stage(FB_PROGRESS_STAGE_REPLAY_WARM, NULL);
	fb_replay_warm_store(info, index, toast_tupdesc, toast_store,
						 &store, backtrack_blocks, result);
	warm_tracked_bytes = Max(index->tracked_bytes, result->tracked_bytes);
	fb_replay_init_result(index, result);
	result->tracked_bytes = warm_tracked_bytes;

	{
		FbReplayControl final_control;
		FbPendingReverseOpQueue pending_ops;

		MemSet(&final_control, 0, sizeof(final_control));
		MemSet(&pending_ops, 0, sizeof(pending_ops));
		final_control.phase = FB_REPLAY_PHASE_FINAL;
		fb_replay_progress_enter(&final_control);
		fb_replay_run_pass(info, index, tupdesc, toast_tupdesc, toast_store,
						   &store, &final_control, result, source, &pending_ops,
						   index->anchor_redo_lsn, InvalidXLogRecPtr);
		fb_pending_reverse_op_flush(&pending_ops, info, tupdesc, toast_store,
									result, source);
		fb_progress_update_percent(FB_PROGRESS_STAGE_REPLAY_FINAL, 100, NULL);
	}

		if (toast_store != NULL)
			fb_toast_store_destroy(toast_store);
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
