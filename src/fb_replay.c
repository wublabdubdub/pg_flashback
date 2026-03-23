#include "postgres.h"

#include "access/heapam_xlog.h"
#include "storage/bufpage.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"

#include "fb_replay.h"

typedef struct FbReplayBlockKey
{
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blkno;
} FbReplayBlockKey;

typedef struct FbReplayBlockState
{
	FbReplayBlockKey key;
	bool initialized;
	char page[BLCKSZ];
	XLogRecPtr page_lsn;
} FbReplayBlockState;

typedef struct FbReplayStore
{
	HTAB *blocks;
} FbReplayStore;

static void
fb_replay_charge_bytes(FbReplayResult *result, Size bytes, const char *what)
{
	if (result == NULL || bytes == 0)
		return;

	if (result->memory_limit_bytes > 0 &&
		result->tracked_bytes + (uint64) bytes > result->memory_limit_bytes)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("pg_flashback memory limit exceeded while tracking %s", what),
				 errdetail("tracked=%llu bytes limit=%llu bytes requested=%zu bytes",
						   (unsigned long long) result->tracked_bytes,
						   (unsigned long long) result->memory_limit_bytes,
						   bytes)));

	result->tracked_bytes += (uint64) bytes;
}

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

static char *
fb_tuple_identity(HeapTuple tuple, TupleDesc tupdesc,
				  const AttrNumber *attrs, int attr_count)
{
	int natts = tupdesc->natts;
	Datum *values;
	bool *nulls;
	StringInfoData buf;
	int i;

	values = palloc0(sizeof(Datum) * natts);
	nulls = palloc0(sizeof(bool) * natts);
	heap_deform_tuple(tuple, tupdesc, values, nulls);

	initStringInfo(&buf);

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		bool include = (attrs == NULL);

		if (attr->attisdropped)
			continue;

		if (!include)
		{
			int key_index;

			for (key_index = 0; key_index < attr_count; key_index++)
			{
				if (attrs[key_index] == attr->attnum)
				{
					include = true;
					break;
				}
			}
		}

		if (!include)
			continue;

		if (buf.len > 0)
			appendStringInfoChar(&buf, '|');

		appendStringInfoString(&buf, NameStr(attr->attname));
		appendStringInfoChar(&buf, '=');

		if (nulls[i])
			appendStringInfoString(&buf, "NULL");
		else
		{
			Oid output_func;
			bool is_varlena;
			char *value_text;
			char *quoted;

			getTypeOutputInfo(attr->atttypid, &output_func, &is_varlena);
			value_text = OidOutputFunctionCall(output_func, values[i]);
			quoted = quote_literal_cstr(value_text);
			appendStringInfoString(&buf, quoted);
		}
	}

	return buf.data;
}

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

static void
fb_row_image_from_tuple(const FbRelationInfo *info, TupleDesc tupdesc,
						 HeapTuple tuple, FbRowImage *row)
{
	MemSet(row, 0, sizeof(*row));
	if (tuple == NULL)
		return;

	row->tuple = heap_copytuple(tuple);
	row->row_identity = fb_tuple_identity(tuple, tupdesc, NULL, 0);
	if (info->mode == FB_APPLY_KEYED && info->key_natts > 0)
		row->key_identity = fb_tuple_identity(tuple, tupdesc,
											  info->key_attnums,
											  info->key_natts);
	else
		row->key_identity = pstrdup(row->row_identity);
}

static void
fb_forward_stream_append(FbForwardOpStream *stream, const FbForwardOp *op)
{
	uint32 old_capacity;

	if (stream == NULL)
		return;

	if (stream->count == stream->capacity)
	{
		old_capacity = stream->capacity;
		stream->capacity = (stream->capacity == 0) ? 32 : stream->capacity * 2;
		if (stream->ops == NULL)
			stream->ops = palloc0(sizeof(FbForwardOp) * stream->capacity);
		else
		{
			stream->ops = repalloc(stream->ops,
								   sizeof(FbForwardOp) * stream->capacity);
			MemSet(stream->ops + old_capacity, 0,
				   sizeof(FbForwardOp) * (stream->capacity - old_capacity));
		}
	}

	stream->ops[stream->count++] = *op;
}

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

static void
fb_replay_ensure_block_ready(FbReplayStore *store,
							 const FbRecordBlockRef *block_ref,
							 bool allow_init,
							 FbReplayResult *result,
							 FbReplayBlockState **state_out)
{
	FbReplayBlockState *state;
	bool found;

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
		result->replay_errors++;
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("missing FPI for block %u", block_ref->blkno)));
	}

	if (state_out != NULL)
		*state_out = state;
}

static void
fb_append_forward_insert(const FbRelationInfo *info, TupleDesc tupdesc,
						 const FbRecordRef *record, HeapTuple new_tuple,
						 FbForwardOpStream *stream)
{
	FbForwardOp op;

	MemSet(&op, 0, sizeof(op));
	op.type = FB_FORWARD_INSERT;
	op.xid = record->xid;
	op.commit_ts = record->commit_ts;
	op.commit_lsn = record->commit_lsn;
	op.record_lsn = record->lsn;
	fb_row_image_from_tuple(info, tupdesc, new_tuple, &op.new_row);
	fb_forward_stream_append(stream, &op);
}

static void
fb_append_forward_delete(const FbRelationInfo *info, TupleDesc tupdesc,
						 const FbRecordRef *record, HeapTuple old_tuple,
						 FbForwardOpStream *stream)
{
	FbForwardOp op;

	MemSet(&op, 0, sizeof(op));
	op.type = FB_FORWARD_DELETE;
	op.xid = record->xid;
	op.commit_ts = record->commit_ts;
	op.commit_lsn = record->commit_lsn;
	op.record_lsn = record->lsn;
	fb_row_image_from_tuple(info, tupdesc, old_tuple, &op.old_row);
	fb_forward_stream_append(stream, &op);
}

static void
fb_append_forward_update(const FbRelationInfo *info, TupleDesc tupdesc,
						 const FbRecordRef *record,
						 HeapTuple old_tuple, HeapTuple new_tuple,
						 FbForwardOpStream *stream)
{
	FbForwardOp op;

	MemSet(&op, 0, sizeof(op));
	op.type = FB_FORWARD_UPDATE;
	op.xid = record->xid;
	op.commit_ts = record->commit_ts;
	op.commit_lsn = record->commit_lsn;
	op.record_lsn = record->lsn;
	fb_row_image_from_tuple(info, tupdesc, old_tuple, &op.old_row);
	fb_row_image_from_tuple(info, tupdesc, new_tuple, &op.new_row);
	fb_forward_stream_append(stream, &op);
}

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

static void
fb_replay_heap_insert(const FbRelationInfo *info,
					  const FbRecordRef *record,
					  FbReplayStore *store,
					  TupleDesc tupdesc,
					  FbReplayResult *result,
					  FbForwardOpStream *stream)
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

	fb_replay_ensure_block_ready(store, block_ref, record->init_page, result, &state);
	page = (Page) state->page;

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
	ItemPointerSetBlockNumber(&target_tid, block_ref->blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);
	htup->t_ctid = target_tid;

	if (PageAddItem(page, (Item) htup, newlen, xlrec->offnum, true, true) == InvalidOffsetNumber)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("failed to replay heap insert")));

	state->page_lsn = record->end_lsn;
	result->records_replayed++;
	if (record->committed_after_target && block_ref->is_main_relation)
		result->target_insert_count++;

	if (stream != NULL && tupdesc != NULL &&
		record->committed_after_target && block_ref->is_main_relation)
	{
		HeapTuple new_tuple = fb_copy_page_tuple(page, block_ref->blkno, xlrec->offnum);

		if (new_tuple != NULL)
			fb_append_forward_insert(info, tupdesc, record, new_tuple, stream);
	}
}

static void
fb_replay_heap_delete(const FbRelationInfo *info,
					  const FbRecordRef *record,
					  FbReplayStore *store,
					  TupleDesc tupdesc,
					  FbReplayResult *result,
					  FbForwardOpStream *stream)
{
	xl_heap_delete *xlrec = (xl_heap_delete *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	HeapTupleHeader htup;
	ItemPointerData target_tid;
	HeapTuple old_tuple = NULL;

	fb_replay_ensure_block_ready(store, block_ref, false, result, &state);
	page = (Page) state->page;

	if (stream != NULL && tupdesc != NULL &&
		record->committed_after_target && block_ref->is_main_relation)
		old_tuple = fb_copy_page_tuple(page, block_ref->blkno, xlrec->offnum);

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
	if (record->committed_after_target && block_ref->is_main_relation)
		result->target_delete_count++;

	if (old_tuple != NULL)
		fb_append_forward_delete(info, tupdesc, record, old_tuple, stream);
}

static void
fb_replay_heap_update(const FbRelationInfo *info,
					  const FbRecordRef *record,
					  FbReplayStore *store,
					  bool hot_update,
					  TupleDesc tupdesc,
					  FbReplayResult *result,
					  FbForwardOpStream *stream)
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
	union
	{
		HeapTupleHeaderData hdr;
		char data[MaxHeapTupleSize];
	} tbuf;

	fb_replay_ensure_block_ready(store, old_block_ref, false, result, &old_state);
	if (old_block_ref == new_block_ref)
		new_state = old_state;
	else
		fb_replay_ensure_block_ready(store, new_block_ref, record->init_page, result, &new_state);

	oldpage = (Page) old_state->page;
	newpage = (Page) new_state->page;
	new_page_image_only = (new_block_ref->has_image &&
						   old_block_ref != new_block_ref);

	if (stream != NULL && tupdesc != NULL &&
		record->committed_after_target && old_block_ref->is_main_relation)
		old_tuple = fb_copy_page_tuple(oldpage, old_block_ref->blkno, xlrec->old_offnum);

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

	if (stream != NULL && tupdesc != NULL &&
		record->committed_after_target && new_block_ref->is_main_relation)
	{
		new_tuple = fb_copy_page_tuple(newpage, new_block_ref->blkno, xlrec->new_offnum);
		if (old_tuple != NULL && new_tuple != NULL)
			fb_append_forward_update(info, tupdesc, record, old_tuple, new_tuple, stream);
	}
}

static void
fb_replay_heap_lock(const FbRecordRef *record,
					FbReplayStore *store,
					FbReplayResult *result)
{
	xl_heap_lock *xlrec = (xl_heap_lock *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	HeapTupleHeader htup;

	fb_replay_ensure_block_ready(store, block_ref, false, result, &state);
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
	HeapTupleHeaderSetXmax(htup, xlrec->xmax);
	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

	state->page_lsn = record->end_lsn;
	result->records_replayed++;
}

static void
fb_replay_heap2_prune(const FbRecordRef *record,
					  FbReplayStore *store,
					  FbReplayResult *result)
{
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];

	fb_replay_ensure_block_ready(store, block_ref, false, result, &state);
	state->page_lsn = record->end_lsn;
	result->records_replayed++;
}

static void
fb_replay_heap_confirm(const FbRecordRef *record,
					   FbReplayStore *store,
					   FbReplayResult *result)
{
	xl_heap_confirm *xlrec = (xl_heap_confirm *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	HeapTupleHeader htup;

	fb_replay_ensure_block_ready(store, block_ref, false, result, &state);
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

static void
fb_replay_heap_inplace(const FbRecordRef *record,
					   FbReplayStore *store,
					   FbReplayResult *result)
{
	xl_heap_inplace *xlrec = (xl_heap_inplace *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	HeapTupleHeader htup;
	uint32 oldlen;

	fb_replay_ensure_block_ready(store, block_ref, false, result, &state);
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

static void
fb_replay_heap2_visible(const FbRecordRef *record,
						FbReplayStore *store,
						FbReplayResult *result)
{
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;

	fb_replay_ensure_block_ready(store, block_ref, false, result, &state);
	page = (Page) state->page;
	PageSetAllVisible(page);
	state->page_lsn = record->end_lsn;
	result->records_replayed++;
}

static void
fb_replay_heap2_lock_updated(const FbRecordRef *record,
							 FbReplayStore *store,
							 FbReplayResult *result)
{
	xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	HeapTupleHeader htup;

	fb_replay_ensure_block_ready(store, block_ref, false, result, &state);
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

static void
fb_replay_heap2_multi_insert(const FbRelationInfo *info,
							 const FbRecordRef *record,
							 FbReplayStore *store,
							 TupleDesc tupdesc,
							 FbReplayResult *result,
							 FbForwardOpStream *stream)
{
	xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *) record->main_data;
	FbReplayBlockState *state;
	FbRecordBlockRef *block_ref = (FbRecordBlockRef *) &record->blocks[0];
	Page page;
	char *tupdata;
	char *endptr;
	int i;
	bool isinit = record->init_page;
	Size len;

	union
	{
		HeapTupleHeaderData hdr;
		char data[MaxHeapTupleSize];
	} tbuf;

	fb_replay_ensure_block_ready(store, block_ref, isinit, result, &state);
	page = (Page) state->page;

	if (!block_ref->has_data || block_ref->data_len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("heap multi insert record missing block data")));

	tupdata = block_ref->data;
	len = block_ref->data_len;
	endptr = tupdata + len;

	for (i = 0; i < xlrec->ntuples; i++)
	{
		OffsetNumber offnum;
		xl_multi_insert_tuple *xlhdr;
		HeapTupleHeader htup;
		uint32 newlen;
		ItemPointerData tid;

		if (isinit)
			offnum = FirstOffsetNumber + i;
		else
			offnum = xlrec->offsets[i];

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

		if (stream != NULL && tupdesc != NULL &&
			record->committed_after_target && block_ref->is_main_relation)
		{
			HeapTuple new_tuple = fb_copy_page_tuple(page, block_ref->blkno, offnum);

			if (new_tuple != NULL)
				fb_append_forward_insert(info, tupdesc, record, new_tuple, stream);
		}
	}

	if (tupdata != endptr)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("heap multi insert tuple length mismatch")));

	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
		PageClearAllVisible(page);
	if (xlrec->flags & XLH_INSERT_ALL_FROZEN_SET)
		PageSetAllVisible(page);

	state->page_lsn = record->end_lsn;
	result->records_replayed++;
	if (record->committed_after_target && block_ref->is_main_relation)
		result->target_insert_count += xlrec->ntuples;
}

static void
fb_replay_execute_internal(const FbRelationInfo *info,
						   const FbWalRecordIndex *index,
						   TupleDesc tupdesc,
						   FbReplayResult *result,
						   FbForwardOpStream *stream)
{
	FbReplayStore store;
	uint32 i;

	MemSet(&store, 0, sizeof(store));
	MemSet(result, 0, sizeof(*result));
	store.blocks = fb_replay_create_block_hash();
	result->tracked_bytes = index->tracked_bytes;
	result->memory_limit_bytes = index->memory_limit_bytes;

	if (stream != NULL)
		MemSet(stream, 0, sizeof(*stream));

	for (i = 0; i < index->record_count; i++)
	{
		const FbRecordRef *record = &index->records[i];

		if (record->lsn < index->anchor_redo_lsn)
			continue;
		if (record->aborted)
			continue;

		switch (record->kind)
		{
			case FB_WAL_RECORD_HEAP_INSERT:
				fb_replay_heap_insert(info, record, &store, tupdesc, result, stream);
				break;
			case FB_WAL_RECORD_HEAP_DELETE:
				fb_replay_heap_delete(info, record, &store, tupdesc, result, stream);
				break;
			case FB_WAL_RECORD_HEAP_UPDATE:
				fb_replay_heap_update(info, record, &store, false, tupdesc, result, stream);
				break;
			case FB_WAL_RECORD_HEAP_HOT_UPDATE:
				fb_replay_heap_update(info, record, &store, true, tupdesc, result, stream);
				break;
			case FB_WAL_RECORD_HEAP_CONFIRM:
				fb_replay_heap_confirm(record, &store, result);
				break;
			case FB_WAL_RECORD_HEAP_LOCK:
				fb_replay_heap_lock(record, &store, result);
				break;
			case FB_WAL_RECORD_HEAP_INPLACE:
				fb_replay_heap_inplace(record, &store, result);
				break;
			case FB_WAL_RECORD_HEAP2_PRUNE:
				fb_replay_heap2_prune(record, &store, result);
				break;
			case FB_WAL_RECORD_HEAP2_VISIBLE:
				fb_replay_heap2_visible(record, &store, result);
				break;
			case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
				fb_replay_heap2_multi_insert(info, record, &store, tupdesc, result, stream);
				break;
			case FB_WAL_RECORD_HEAP2_LOCK_UPDATED:
				fb_replay_heap2_lock_updated(record, &store, result);
				break;
		}
	}
}

void
fb_replay_execute(const FbRelationInfo *info,
				  const FbWalRecordIndex *index,
				  FbReplayResult *result)
{
	fb_replay_execute_internal(info, index, NULL, result, NULL);
}

void
fb_replay_build_forward_ops(const FbRelationInfo *info,
							const FbWalRecordIndex *index,
							TupleDesc tupdesc,
							FbReplayResult *result,
							FbForwardOpStream *stream)
{
	fb_replay_execute_internal(info, index, tupdesc, result, stream);
}

char *
fb_replay_debug_summary(const FbReplayResult *result)
{
	return psprintf("replayed=%llu blocks=%llu errors=%llu inserts=%llu deletes=%llu updates=%llu",
					(unsigned long long) result->records_replayed,
					(unsigned long long) result->blocks_materialized,
					(unsigned long long) result->replay_errors,
					(unsigned long long) result->target_insert_count,
					(unsigned long long) result->target_delete_count,
					(unsigned long long) result->target_update_count);
}
