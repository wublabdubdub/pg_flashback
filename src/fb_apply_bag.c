/*
 * fb_apply_bag.c
 *    Bag-semantic streaming reverse-op application helpers.
 */

#include "postgres.h"

#include "access/hash.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"

#include "fb_apply.h"
#include "fb_memory.h"

typedef struct FbBagBucket
{
	uint32 hash;
	void *head;
} FbBagBucket;

typedef struct FbBagEntry
{
	char *row_identity;
	int64 delta;
	HeapTuple tuple;
	struct FbBagEntry *bucket_next;
	struct FbBagEntry *all_next;
} FbBagEntry;

typedef struct FbBagApplyState
{
	TupleDesc tupdesc;
	HTAB *buckets;
	FbBagEntry *entries_head;
	FbBagEntry *entries_tail;
	FbBagEntry *residual_cursor;
	int64 residual_repeat;
	uint64 residual_total;
	uint64 tracked_bytes;
	uint64 memory_limit_bytes;
} FbBagApplyState;

/*
 * fb_bag_charge_bytes
 *    Bag apply helper.
 */

static void
fb_bag_charge_bytes(FbBagApplyState *state, Size bytes, const char *what)
{
	if (state == NULL)
		return;

	fb_memory_charge_bytes(&state->tracked_bytes,
						   state->memory_limit_bytes,
						   bytes,
						   what);
}

/*
 * fb_bag_create_bucket_hash
 *    Bag apply helper.
 */

static HTAB *
fb_bag_create_bucket_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(FbBagBucket);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create("fb bag changed rows", 128, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * fb_apply_build_row_identity
 *    Apply entry point.
 */

char *
fb_apply_build_row_identity(HeapTuple tuple, TupleDesc tupdesc)
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

		if (attr->attisdropped)
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

/*
 * fb_bag_find_entry
 *    Bag apply helper.
 */

static FbBagEntry *
fb_bag_find_entry(HTAB *hash, const char *identity)
{
	FbBagBucket *bucket;
	FbBagEntry *entry;
	uint32 hash_value;

	hash_value = fb_apply_hash_identity(identity);
	bucket = (FbBagBucket *) hash_search(hash, &hash_value, HASH_FIND, NULL);
	if (bucket == NULL)
		return NULL;

	for (entry = (FbBagEntry *) bucket->head;
		 entry != NULL;
		 entry = entry->bucket_next)
	{
		if (strcmp(entry->row_identity, identity) == 0)
			return entry;
	}

	return NULL;
}

/*
 * fb_bag_get_or_create_entry
 *    Bag apply helper.
 */

static FbBagEntry *
fb_bag_get_or_create_entry(FbBagApplyState *state, const char *identity)
{
	FbBagBucket *bucket;
	FbBagEntry *entry;
	uint32 hash_value;
	bool found;

	entry = fb_bag_find_entry(state->buckets, identity);
	if (entry != NULL)
		return entry;

	hash_value = fb_apply_hash_identity(identity);
	bucket = (FbBagBucket *) hash_search(state->buckets, &hash_value, HASH_ENTER, &found);
	if (!found)
		bucket->head = NULL;

	entry = palloc0(sizeof(*entry));
	fb_bag_charge_bytes(state, sizeof(*entry), "apply bag delta entry");
	entry->row_identity = pstrdup(identity);
	fb_bag_charge_bytes(state,
						fb_memory_cstring_bytes(entry->row_identity),
						"apply bag row identity");
	entry->bucket_next = (FbBagEntry *) bucket->head;
	bucket->head = entry;

	if (state->entries_tail == NULL)
		state->entries_head = entry;
	else
		state->entries_tail->all_next = entry;
	state->entries_tail = entry;

	return entry;
}

/*
 * fb_bag_adjust_delta
 *    Bag apply helper.
 */

static void
fb_bag_adjust_delta(FbBagApplyState *state,
					 const char *identity,
					 HeapTuple tuple,
					 int64 delta)
{
	FbBagEntry *entry;

	if (identity == NULL)
		return;

	entry = fb_bag_get_or_create_entry(state, identity);
	entry->delta += delta;
	if (delta > 0 && tuple != NULL)
		entry->tuple = tuple;
}

/*
 * fb_bag_apply_begin
 *    Bag apply entry point.
 */

void *
fb_bag_apply_begin(const FbRelationInfo *info,
				   TupleDesc tupdesc,
				   const FbReverseOpStream *stream)
{
	FbBagApplyState *state;
	uint32 i;

	(void) info;

	state = palloc0(sizeof(*state));
	state->tupdesc = tupdesc;
	state->buckets = fb_bag_create_bucket_hash();
	state->tracked_bytes = stream->tracked_bytes;
	state->memory_limit_bytes = stream->memory_limit_bytes;

	for (i = 0; i < stream->count; i++)
	{
		const FbReverseOp *op = &stream->ops[i];

		switch (op->type)
		{
			case FB_REVERSE_REMOVE:
				fb_bag_adjust_delta(state,
									op->new_row.row_identity,
									op->new_row.tuple,
									-1);
				break;
			case FB_REVERSE_ADD:
				fb_bag_adjust_delta(state,
									op->old_row.row_identity,
									op->old_row.tuple,
									1);
				break;
			case FB_REVERSE_REPLACE:
				fb_bag_adjust_delta(state,
									op->new_row.row_identity,
									op->new_row.tuple,
									-1);
				fb_bag_adjust_delta(state,
									op->old_row.row_identity,
									op->old_row.tuple,
									1);
				break;
		}
	}

	return state;
}

/*
 * fb_bag_apply_process_current
 *    Bag apply entry point.
 */

HeapTuple
fb_bag_apply_process_current(void *arg, HeapTuple tuple)
{
	FbBagApplyState *state = (FbBagApplyState *) arg;
	FbBagEntry *entry;
	char *identity;

	identity = fb_apply_build_row_identity(tuple, state->tupdesc);
	entry = fb_bag_find_entry(state->buckets, identity);
	pfree(identity);

	if (entry == NULL)
		return tuple;

	if (entry->delta < 0)
	{
		entry->delta++;
		heap_freetuple(tuple);
		return NULL;
	}

	return tuple;
}

/*
 * fb_bag_apply_finish_scan
 *    Bag apply entry point.
 */

void
fb_bag_apply_finish_scan(void *arg)
{
	FbBagApplyState *state = (FbBagApplyState *) arg;
	FbBagEntry *entry;

	state->residual_total = 0;
	for (entry = state->entries_head; entry != NULL; entry = entry->all_next)
	{
		if (entry->delta < 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("fb bag apply consumed fewer current rows than reverse ops require")));
		if (entry->delta > 0 && entry->tuple == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("fb bag apply is missing representative tuple for residual rows")));
		state->residual_total += (uint64) Max(entry->delta, 0);
	}

	state->residual_cursor = state->entries_head;
	state->residual_repeat = 0;
}

/*
 * fb_bag_apply_residual_total
 *    Bag apply entry point.
 */

uint64
fb_bag_apply_residual_total(void *arg)
{
	FbBagApplyState *state = (FbBagApplyState *) arg;

	return state->residual_total;
}

/*
 * fb_bag_apply_next_residual
 *    Bag apply entry point.
 */

HeapTuple
fb_bag_apply_next_residual(void *arg)
{
	FbBagApplyState *state = (FbBagApplyState *) arg;

	while (state->residual_cursor != NULL)
	{
		if (state->residual_repeat > 0)
		{
			state->residual_repeat--;
			return state->residual_cursor->tuple;
		}

		state->residual_cursor = state->residual_cursor->all_next;
		while (state->residual_cursor != NULL)
		{
			if (state->residual_cursor->delta > 0)
			{
				state->residual_repeat = state->residual_cursor->delta - 1;
				return state->residual_cursor->tuple;
			}

			state->residual_cursor = state->residual_cursor->all_next;
		}
	}

	return NULL;
}

/*
 * fb_bag_apply_end
 *    Bag apply entry point.
 */

void
fb_bag_apply_end(void *arg)
{
	(void) arg;
}
