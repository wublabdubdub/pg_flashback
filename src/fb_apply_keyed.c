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

static uint32
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
fb_keyed_upsert_row(HTAB *hash, const char *identity, HeapTuple tuple)
{
	FbKeyBucket *bucket;
	FbKeyedRow *row;
	uint32 hash_value;
	bool found;

	row = fb_keyed_find_row(hash, identity);
	if (row != NULL)
	{
		row->tuple = heap_copytuple(tuple);
		return;
	}

	hash_value = fb_apply_hash_identity(identity);
	bucket = (FbKeyBucket *) hash_search(hash, &hash_value, HASH_ENTER, &found);
	if (!found)
		bucket->head = NULL;

	row = palloc0(sizeof(*row));
	row->key_identity = pstrdup(identity);
	row->tuple = heap_copytuple(tuple);
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

typedef struct FbKeyedLoadState
{
	const FbRelationInfo *info;
	TupleDesc tupdesc;
	HTAB *rows;
} FbKeyedLoadState;

static void
fb_keyed_put_tuple(Tuplestorestate *tuplestore, TupleDesc tupdesc, HeapTuple tuple)
{
	int natts = tupdesc->natts;
	Datum *values;
	bool *nulls;

	values = palloc0(sizeof(Datum) * natts);
	nulls = palloc0(sizeof(bool) * natts);
	heap_deform_tuple(tuple, tupdesc, values, nulls);
	tuplestore_putvalues(tuplestore, tupdesc, values, nulls);
}

static void
fb_keyed_load_current_tuple(HeapTuple tuple, TupleDesc tupdesc, void *arg)
{
	FbKeyedLoadState *state = (FbKeyedLoadState *) arg;
	char *identity;

	(void) tupdesc;

	identity = fb_apply_tuple_identity(tuple, state->tupdesc,
									   state->info->key_attnums,
									   state->info->key_natts);
	fb_keyed_upsert_row(state->rows, identity, tuple);
}

void
fb_apply_scan_current_relation(Oid relid, FbCurrentTupleVisitor visitor, void *arg)
{
	Relation rel;
	Snapshot snapshot;
	bool pushed_snapshot = false;
	TableScanDesc scan;
	TupleTableSlot *slot;

	rel = relation_open(relid, AccessShareLock);
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
		ExecClearTuple(slot);
	}

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
	relation_close(rel, AccessShareLock);

	if (pushed_snapshot)
		PopActiveSnapshot();
}

void
fb_apply_keyed_mode(const FbRelationInfo *info,
					 TupleDesc tupdesc,
					 const FbReverseOpStream *stream,
					 Tuplestorestate *tuplestore)
{
	FbKeyedLoadState state;
	uint32 i;
	HASH_SEQ_STATUS seq;
	FbKeyBucket *bucket;

	MemSet(&state, 0, sizeof(state));
	state.info = info;
	state.tupdesc = tupdesc;
	state.rows = fb_apply_create_bucket_hash("fb keyed rows");

	fb_apply_scan_current_relation(info->relid, fb_keyed_load_current_tuple, &state);

	for (i = 0; i < stream->count; i++)
	{
		const FbReverseOp *op = &stream->ops[i];

		switch (op->type)
		{
			case FB_REVERSE_REMOVE:
				if (op->new_row.key_identity != NULL)
					fb_keyed_remove_row(state.rows, op->new_row.key_identity);
				break;
			case FB_REVERSE_ADD:
				if (op->old_row.key_identity != NULL && op->old_row.tuple != NULL)
					fb_keyed_upsert_row(state.rows, op->old_row.key_identity,
										op->old_row.tuple);
				break;
			case FB_REVERSE_REPLACE:
				if (op->new_row.key_identity != NULL)
					fb_keyed_remove_row(state.rows, op->new_row.key_identity);
				if (op->old_row.key_identity != NULL && op->old_row.tuple != NULL)
					fb_keyed_upsert_row(state.rows, op->old_row.key_identity,
										op->old_row.tuple);
				break;
		}
	}

	hash_seq_init(&seq, state.rows);
	while ((bucket = (FbKeyBucket *) hash_seq_search(&seq)) != NULL)
	{
		FbKeyedRow *row;

		for (row = (FbKeyedRow *) bucket->head; row != NULL; row = row->next)
			fb_keyed_put_tuple(tuplestore, tupdesc, row->tuple);
	}
}

void
fb_apply_reverse_ops(const FbRelationInfo *info,
					  TupleDesc tupdesc,
					  const FbReverseOpStream *stream,
					  Tuplestorestate *tuplestore)
{
	if (info->mode == FB_APPLY_KEYED)
		fb_apply_keyed_mode(info, tupdesc, stream, tuplestore);
	else
		fb_apply_bag_mode(info, tupdesc, stream, tuplestore);
}
