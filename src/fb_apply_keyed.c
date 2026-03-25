#include "postgres.h"

#include "access/relation.h"
#include "access/tableam.h"
#include "common/hashfn.h"
#include "executor/tuptable.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

#include "fb_apply.h"
#include "fb_memory.h"
#include "fb_progress.h"

typedef struct FbKeyBucket
{
	uint32 hash;
	void *head;
} FbKeyBucket;

typedef struct FbKeyedRow
{
	char *key_identity;
	HeapTuple tuple;
	struct FbKeyedRow *next;
} FbKeyedRow;

struct FbKeyedState
{
	const FbRelationInfo *info;
	TupleDesc tupdesc;
	HTAB *rows;
	uint64 tracked_bytes;
	uint64 memory_limit_bytes;
};

static void
fb_keyed_charge_bytes(FbKeyedState *state, Size bytes, const char *what)
{
	if (state == NULL)
		return;

	fb_memory_charge_bytes(&state->tracked_bytes,
						   state->memory_limit_bytes,
						   bytes,
						   what);
}

static HTAB *
fb_apply_create_bucket_hash(const char *name)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(FbKeyBucket);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create(name, 128, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

uint32
fb_apply_hash_identity(const char *identity)
{
	if (identity == NULL)
		return 0;

	return DatumGetUInt32(hash_any((const unsigned char *) identity,
								   strlen(identity)));
}

static char *
fb_apply_tuple_identity(HeapTuple tuple, TupleDesc tupdesc,
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

static FbKeyedRow *
fb_keyed_find_row(HTAB *hash, const char *identity)
{
	FbKeyBucket *bucket;
	FbKeyedRow *row;
	uint32 hash_value;

	hash_value = fb_apply_hash_identity(identity);
	bucket = (FbKeyBucket *) hash_search(hash, &hash_value, HASH_FIND, NULL);
	if (bucket == NULL)
		return NULL;

	for (row = (FbKeyedRow *) bucket->head; row != NULL; row = row->next)
	{
		if (strcmp(row->key_identity, identity) == 0)
			return row;
	}

	return NULL;
}

static void
fb_keyed_upsert_row(FbKeyedState *state,
					const char *identity,
					HeapTuple tuple,
					bool identity_owned,
					bool tuple_owned)
{
	FbKeyBucket *bucket;
	FbKeyedRow *row;
	uint32 hash_value;
	bool found;

	row = fb_keyed_find_row(state->rows, identity);
	if (row != NULL)
	{
		if (tuple != NULL)
			row->tuple = tuple;
		if (identity_owned)
			pfree((char *) identity);
		return;
	}

	hash_value = fb_apply_hash_identity(identity);
	bucket = (FbKeyBucket *) hash_search(state->rows, &hash_value, HASH_ENTER, &found);
	if (!found)
		bucket->head = NULL;

	row = palloc0(sizeof(*row));
	fb_keyed_charge_bytes(state, sizeof(*row), "apply keyed row state");
	row->key_identity = (char *) identity;
	row->tuple = tuple;
	if (identity_owned)
		fb_keyed_charge_bytes(state,
							  fb_memory_cstring_bytes(identity),
							  "apply keyed row identity");
	if (tuple_owned)
		fb_keyed_charge_bytes(state,
							  fb_memory_heaptuple_bytes(tuple),
							  "apply keyed tuple");
	row->next = (FbKeyedRow *) bucket->head;
	bucket->head = row;
}

static void
fb_keyed_remove_row(HTAB *hash, const char *identity)
{
	FbKeyBucket *bucket;
	FbKeyedRow *row;
	FbKeyedRow *prev = NULL;
	uint32 hash_value;

	hash_value = fb_apply_hash_identity(identity);
	bucket = (FbKeyBucket *) hash_search(hash, &hash_value, HASH_FIND, NULL);
	if (bucket == NULL)
		return;

	for (row = (FbKeyedRow *) bucket->head; row != NULL; row = row->next)
	{
		if (strcmp(row->key_identity, identity) == 0)
		{
			if (prev == NULL)
				bucket->head = row->next;
			else
				prev->next = row->next;
			return;
		}
		prev = row;
	}
}

char *
fb_apply_build_key_identity(const FbRelationInfo *info,
							  HeapTuple tuple,
							  TupleDesc tupdesc)
{
	return fb_apply_tuple_identity(tuple,
								   tupdesc,
								   info->key_attnums,
								   info->key_natts);
}

static void
fb_result_sink_put_tuple(const FbResultSink *sink, HeapTuple tuple, TupleDesc tupdesc)
{
	if (sink == NULL || sink->put_tuple == NULL)
		elog(ERROR, "fb result sink must not be NULL");

	sink->put_tuple(tuple, tupdesc, sink->arg);
}

static void
fb_keyed_load_current_tuple(HeapTuple tuple, TupleDesc tupdesc, void *arg)
{
	(void) tupdesc;
	fb_keyed_state_add_current_tuple((FbKeyedState *) arg, tuple);
}

void
fb_apply_scan_current_relation(Oid relid, FbCurrentTupleVisitor visitor, void *arg,
								const char *progress_detail)
{
	Relation rel;
	Snapshot snapshot;
	bool pushed_snapshot = false;
	TableScanDesc scan;
	TupleTableSlot *slot;
	double estimated_rows;
	uint64 scanned = 0;

	rel = relation_open(relid, AccessShareLock);
	estimated_rows = rel->rd_rel->reltuples;
	snapshot = GetActiveSnapshot();
	if (snapshot == NULL)
	{
		PushActiveSnapshot(GetLatestSnapshot());
		snapshot = GetActiveSnapshot();
		pushed_snapshot = true;
	}

	scan = table_beginscan(rel, snapshot, 0, NULL);
	slot = table_slot_create(rel, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		HeapTuple tuple = ExecCopySlotHeapTuple(slot);

		visitor(tuple, RelationGetDescr(rel), arg);
		scanned++;
		if (estimated_rows > 0)
		{
			uint32 percent = fb_progress_map_subrange(0, 40,
													  scanned,
													  (uint64) estimated_rows);
			fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY,
									   percent,
									   progress_detail);
		}
		ExecClearTuple(slot);
	}

	fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY, 40, progress_detail);

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
	relation_close(rel, AccessShareLock);

	if (pushed_snapshot)
		PopActiveSnapshot();
}

static uint64
fb_keyed_count_rows(HTAB *rows)
{
	HASH_SEQ_STATUS seq;
	FbKeyBucket *bucket;
	uint64 total = 0;

	hash_seq_init(&seq, rows);
	while ((bucket = (FbKeyBucket *) hash_seq_search(&seq)) != NULL)
	{
		FbKeyedRow *row;

		for (row = (FbKeyedRow *) bucket->head; row != NULL; row = row->next)
			total++;
	}

	return total;
}

FbKeyedState *
fb_keyed_state_create(const FbRelationInfo *info,
					   TupleDesc tupdesc,
					   uint64 tracked_bytes,
					   uint64 memory_limit_bytes)
{
	FbKeyedState *state = palloc0(sizeof(*state));

	state->info = info;
	state->tupdesc = tupdesc;
	state->rows = fb_apply_create_bucket_hash("fb keyed rows");
	state->tracked_bytes = tracked_bytes;
	state->memory_limit_bytes = memory_limit_bytes;
	return state;
}

void
fb_keyed_state_add_current_tuple(FbKeyedState *state, HeapTuple tuple)
{
	char *identity;

	identity = fb_apply_build_key_identity(state->info, tuple, state->tupdesc);
	fb_keyed_upsert_row(state, identity, tuple, true, true);
}

void
fb_keyed_state_apply_add_tuple(FbKeyedState *state, HeapTuple tuple)
{
	char *identity;

	identity = fb_apply_build_key_identity(state->info, tuple, state->tupdesc);
	fb_keyed_upsert_row(state, identity, tuple, true, true);
}

void
fb_keyed_state_apply_remove_identity(FbKeyedState *state, const char *identity)
{
	if (identity != NULL)
		fb_keyed_remove_row(state->rows, identity);
}

void
fb_keyed_state_apply_remove_tuple(FbKeyedState *state, HeapTuple tuple)
{
	char *identity;

	identity = fb_apply_build_key_identity(state->info, tuple, state->tupdesc);
	fb_keyed_remove_row(state->rows, identity);
	pfree(identity);
}

uint64
fb_keyed_state_count_rows(FbKeyedState *state)
{
	return fb_keyed_count_rows(state->rows);
}

uint64
fb_keyed_state_emit_rows(FbKeyedState *state,
						  TupleDesc tupdesc,
						  const FbResultSink *sink)
{
	HASH_SEQ_STATUS seq;
	FbKeyBucket *bucket;
	uint64 emitted = 0;
	uint64 total_rows;

	total_rows = fb_keyed_state_count_rows(state);
	if (total_rows == 0)
		return 0;

	hash_seq_init(&seq, state->rows);
	while ((bucket = (FbKeyBucket *) hash_seq_search(&seq)) != NULL)
	{
		FbKeyedRow *row;

		for (row = (FbKeyedRow *) bucket->head; row != NULL; row = row->next)
		{
			fb_result_sink_put_tuple(sink, row->tuple, tupdesc);
			emitted++;
		}
	}

	return emitted;
}

uint64
fb_apply_keyed_mode(const FbRelationInfo *info,
					 TupleDesc tupdesc,
					 const FbReverseOpStream *stream,
					 const FbResultSink *sink)
{
	FbKeyedState *state;
	uint32 i;
	uint64 emitted;
	uint64 total_rows;

	state = fb_keyed_state_create(info,
								  tupdesc,
								  stream->tracked_bytes,
								  stream->memory_limit_bytes);

	fb_apply_scan_current_relation(info->relid,
								   fb_keyed_load_current_tuple,
								   state,
								   NULL);
	if (stream->count == 0)
		fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY, 100, NULL);

	for (i = 0; i < stream->count; i++)
	{
		const FbReverseOp *op = &stream->ops[i];

		switch (op->type)
		{
			case FB_REVERSE_REMOVE:
				if (op->new_row.key_identity != NULL)
					fb_keyed_state_apply_remove_identity(state,
														 op->new_row.key_identity);
				break;
			case FB_REVERSE_ADD:
				if (op->old_row.tuple != NULL)
					fb_keyed_state_apply_add_tuple(state, op->old_row.tuple);
				break;
			case FB_REVERSE_REPLACE:
				if (op->new_row.key_identity != NULL)
					fb_keyed_state_apply_remove_identity(state,
														 op->new_row.key_identity);
				if (op->old_row.tuple != NULL)
					fb_keyed_state_apply_add_tuple(state, op->old_row.tuple);
				break;
		}

		fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY,
								   fb_progress_map_subrange(40, 60,
															(uint64) i + 1,
															stream->count),
								   NULL);
	}

	total_rows = fb_keyed_state_count_rows(state);
	fb_progress_enter_stage(FB_PROGRESS_STAGE_MATERIALIZE, NULL);
	if (total_rows == 0)
	{
		fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);
		return 0;
	}

	emitted = 0;
	{
		HASH_SEQ_STATUS seq;
		FbKeyBucket *bucket;

		hash_seq_init(&seq, state->rows);
		while ((bucket = (FbKeyBucket *) hash_seq_search(&seq)) != NULL)
		{
			FbKeyedRow *row;

			for (row = (FbKeyedRow *) bucket->head; row != NULL; row = row->next)
			{
				fb_result_sink_put_tuple(sink, row->tuple, tupdesc);
				emitted++;
				fb_progress_update_fraction(FB_PROGRESS_STAGE_MATERIALIZE,
											 emitted,
											 total_rows,
											 NULL);
			}
		}
	}

	return emitted;
}

uint64
fb_apply_reverse_ops(const FbRelationInfo *info,
					  TupleDesc tupdesc,
					  const FbReverseOpStream *stream,
					  const FbResultSink *sink)
{
	fb_progress_enter_stage(FB_PROGRESS_STAGE_APPLY, NULL);
	if (info->mode == FB_APPLY_KEYED)
		return fb_apply_keyed_mode(info, tupdesc, stream, sink);

	return fb_apply_bag_mode(info, tupdesc, stream, sink);
}
