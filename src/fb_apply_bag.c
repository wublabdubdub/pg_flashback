#include "postgres.h"

#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"

#include "fb_apply.h"
#include "fb_memory.h"
#include "fb_progress.h"

typedef struct FbBagBucket
{
	uint32 hash;
	void *head;
} FbBagBucket;

typedef struct FbBagRow
{
	char *row_identity;
	HeapTuple tuple;
	int64 count;
	struct FbBagRow *next;
} FbBagRow;

struct FbBagState
{
	TupleDesc tupdesc;
	HTAB *rows;
	uint64 tracked_bytes;
	uint64 memory_limit_bytes;
};

static void
fb_bag_charge_bytes(FbBagState *state, Size bytes, const char *what)
{
	if (state == NULL)
		return;

	fb_memory_charge_bytes(&state->tracked_bytes,
						   state->memory_limit_bytes,
						   bytes,
						   what);
}

static HTAB *
fb_bag_create_bucket_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(FbBagBucket);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create("fb bag rows", 128, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static uint32
fb_bag_hash_identity(const char *identity)
{
	if (identity == NULL)
		return 0;

	return DatumGetUInt32(hash_any((const unsigned char *) identity,
								   strlen(identity)));
}

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

static FbBagRow *
fb_bag_find_row(HTAB *hash, const char *identity)
{
	FbBagBucket *bucket;
	FbBagRow *row;
	uint32 hash_value;

	hash_value = fb_bag_hash_identity(identity);
	bucket = (FbBagBucket *) hash_search(hash, &hash_value, HASH_FIND, NULL);
	if (bucket == NULL)
		return NULL;

	for (row = (FbBagRow *) bucket->head; row != NULL; row = row->next)
	{
		if (strcmp(row->row_identity, identity) == 0)
			return row;
	}

	return NULL;
}

static void
fb_bag_adjust_row(FbBagState *state,
				  const char *identity,
				  HeapTuple tuple,
				  int64 delta,
				  bool identity_owned,
				  bool tuple_owned)
{
	FbBagBucket *bucket;
	FbBagRow *row;
	uint32 hash_value;
	bool found;

	row = fb_bag_find_row(state->rows, identity);
	if (row == NULL)
	{
		hash_value = fb_bag_hash_identity(identity);
		bucket = (FbBagBucket *) hash_search(state->rows, &hash_value, HASH_ENTER, &found);
		if (!found)
			bucket->head = NULL;

		row = palloc0(sizeof(*row));
		fb_bag_charge_bytes(state, sizeof(*row), "apply bag row state");
		row->row_identity = (char *) identity;
		row->tuple = tuple;
		if (identity_owned)
			fb_bag_charge_bytes(state,
								fb_memory_cstring_bytes(identity),
								"apply bag row identity");
		if (tuple_owned)
			fb_bag_charge_bytes(state,
								fb_memory_heaptuple_bytes(tuple),
								"apply bag tuple");
		row->next = (FbBagRow *) bucket->head;
		bucket->head = row;
	}
	else if (row->tuple == NULL && tuple != NULL)
	{
		row->tuple = tuple;
		if (tuple_owned)
			fb_bag_charge_bytes(state,
								fb_memory_heaptuple_bytes(tuple),
								"apply bag tuple");
	}
	else
	{
		if (identity_owned)
			pfree((char *) identity);
		if (tuple_owned && tuple != NULL)
			heap_freetuple(tuple);
	}

	row->count += delta;
}

static void
fb_result_sink_put_tuple(const FbResultSink *sink, HeapTuple tuple, TupleDesc tupdesc)
{
	if (sink == NULL || sink->put_tuple == NULL)
		elog(ERROR, "fb result sink must not be NULL");

	sink->put_tuple(tuple, tupdesc, sink->arg);
}

static void
fb_bag_load_current_tuple(HeapTuple tuple, TupleDesc tupdesc, void *arg)
{
	(void) tupdesc;
	fb_bag_state_add_current_tuple((FbBagState *) arg, tuple);
}

static uint64
fb_bag_count_rows(HTAB *rows)
{
	HASH_SEQ_STATUS seq;
	FbBagBucket *bucket;
	uint64 total = 0;

	hash_seq_init(&seq, rows);
	while ((bucket = (FbBagBucket *) hash_seq_search(&seq)) != NULL)
	{
		FbBagRow *row;

		for (row = (FbBagRow *) bucket->head; row != NULL; row = row->next)
		{
			if (row->count > 0 && row->tuple != NULL)
				total += (uint64) row->count;
		}
	}

	return total;
}

FbBagState *
fb_bag_state_create(TupleDesc tupdesc,
					 uint64 tracked_bytes,
					 uint64 memory_limit_bytes)
{
	FbBagState *state = palloc0(sizeof(*state));

	state->tupdesc = tupdesc;
	state->rows = fb_bag_create_bucket_hash();
	state->tracked_bytes = tracked_bytes;
	state->memory_limit_bytes = memory_limit_bytes;
	return state;
}

void
fb_bag_state_add_current_tuple(FbBagState *state, HeapTuple tuple)
{
	char *identity;

	identity = fb_apply_build_row_identity(tuple, state->tupdesc);
	fb_bag_adjust_row(state, identity, tuple, 1, true, true);
}

void
fb_bag_state_apply_add_tuple(FbBagState *state, HeapTuple tuple)
{
	char *identity;

	identity = fb_apply_build_row_identity(tuple, state->tupdesc);
	fb_bag_adjust_row(state, identity, tuple, 1, true, true);
}

void
fb_bag_state_apply_remove_tuple(FbBagState *state, HeapTuple tuple)
{
	char *identity;

	identity = fb_apply_build_row_identity(tuple, state->tupdesc);
	fb_bag_adjust_row(state, identity, tuple, -1, true, true);
}

uint64
fb_bag_state_count_rows(FbBagState *state)
{
	return fb_bag_count_rows(state->rows);
}

uint64
fb_bag_state_emit_rows(FbBagState *state,
						TupleDesc tupdesc,
						const FbResultSink *sink)
{
	HASH_SEQ_STATUS seq;
	FbBagBucket *bucket;
	uint64 emitted = 0;

	hash_seq_init(&seq, state->rows);
	while ((bucket = (FbBagBucket *) hash_seq_search(&seq)) != NULL)
	{
		FbBagRow *row;

		for (row = (FbBagRow *) bucket->head; row != NULL; row = row->next)
		{
			int64 j;

			if (row->count <= 0 || row->tuple == NULL)
				continue;

			for (j = 0; j < row->count; j++)
			{
				fb_result_sink_put_tuple(sink, row->tuple, tupdesc);
				emitted++;
			}
		}
	}

	return emitted;
}

uint64
fb_apply_bag_mode(const FbRelationInfo *info,
				   TupleDesc tupdesc,
				   const FbReverseOpStream *stream,
				   const FbResultSink *sink)
{
	FbBagState *state;
	uint32 i;
	uint64 emitted;
	uint64 total_rows;

	(void) info;

	state = fb_bag_state_create(tupdesc,
								stream->tracked_bytes,
								stream->memory_limit_bytes);

	fb_apply_scan_current_relation(info->relid,
								   fb_bag_load_current_tuple,
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
				if (op->new_row.tuple != NULL)
					fb_bag_state_apply_remove_tuple(state, op->new_row.tuple);
				break;
			case FB_REVERSE_ADD:
				if (op->old_row.tuple != NULL)
					fb_bag_state_apply_add_tuple(state, op->old_row.tuple);
				break;
			case FB_REVERSE_REPLACE:
				if (op->new_row.tuple != NULL)
					fb_bag_state_apply_remove_tuple(state, op->new_row.tuple);
				if (op->old_row.tuple != NULL)
					fb_bag_state_apply_add_tuple(state, op->old_row.tuple);
				break;
		}

		fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY,
								   fb_progress_map_subrange(40, 60,
															(uint64) i + 1,
															stream->count),
								   NULL);
	}

	total_rows = fb_bag_state_count_rows(state);
	fb_progress_enter_stage(FB_PROGRESS_STAGE_MATERIALIZE, NULL);
	if (total_rows == 0)
	{
		fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);
		return 0;
	}

	emitted = 0;
	{
		HASH_SEQ_STATUS seq;
		FbBagBucket *bucket;

		hash_seq_init(&seq, state->rows);
		while ((bucket = (FbBagBucket *) hash_seq_search(&seq)) != NULL)
		{
			FbBagRow *row;

			for (row = (FbBagRow *) bucket->head; row != NULL; row = row->next)
			{
				int64 j;

				if (row->count <= 0 || row->tuple == NULL)
					continue;

				for (j = 0; j < row->count; j++)
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
	}

	return emitted;
}
