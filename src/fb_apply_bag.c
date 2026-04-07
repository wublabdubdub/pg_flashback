/*
 * fb_apply_bag.c
 *    Bag-semantic streaming reverse-op application helpers.
 */

#include "postgres.h"

#include <limits.h>

#include "access/hash.h"
#include "common/hashfn.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/uuid.h"

#include "fb_apply.h"
#include "fb_memory.h"

typedef enum FbBagFastKind
{
	FB_BAG_FAST_GENERIC = 0,
	FB_BAG_FAST_BOOL,
	FB_BAG_FAST_INT2,
	FB_BAG_FAST_INT4,
	FB_BAG_FAST_INT8,
	FB_BAG_FAST_OID,
	FB_BAG_FAST_UUID
} FbBagFastKind;

typedef struct FbBagBucket
{
	uint32 hash;
	void *head;
} FbBagBucket;

typedef struct FbBagAttrMeta
{
	AttrNumber attnum;
	int16 attlen;
	bool attbyval;
	Oid attcollation;
	FbBagFastKind fast_kind;
	FmgrInfo hash_fmgr;
	FmgrInfo eq_fmgr;
} FbBagAttrMeta;

typedef struct FbBagEntry
{
	char *row_identity;
	Datum *values;
	bool *nulls;
	uint32 hash_value;
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
	bool use_typed_identity;
	int attr_count;
	int max_attnum;
	FbBagAttrMeta *attr_meta;
	Datum *scratch_values;
	bool *scratch_nulls;
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
fb_bag_create_bucket_hash(long initial_size)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(FbBagBucket);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create("fb bag changed rows", Max(initial_size, 128L), &ctl,
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

static long
fb_bag_bucket_target(const FbReverseOpSource *source)
{
	uint64 total = fb_reverse_source_total_count(source);

	if (total < 128)
		return 128;
	if (total > (uint64) LONG_MAX)
		return LONG_MAX;
	return (long) total;
}

static FbBagFastKind
fb_bag_fast_kind_for_type(Oid type_oid)
{
	switch (type_oid)
	{
		case BOOLOID:
			return FB_BAG_FAST_BOOL;
		case INT2OID:
			return FB_BAG_FAST_INT2;
		case INT4OID:
			return FB_BAG_FAST_INT4;
		case INT8OID:
			return FB_BAG_FAST_INT8;
		case OIDOID:
			return FB_BAG_FAST_OID;
		case UUIDOID:
			return FB_BAG_FAST_UUID;
		default:
			return FB_BAG_FAST_GENERIC;
	}
}

static void
fb_bag_init_attr_meta(FbBagApplyState *state)
{
	int i;

	state->use_typed_identity = false;
	state->attr_count = 0;
	state->max_attnum = 0;
	if (state->tupdesc == NULL)
		return;

	state->attr_meta = palloc0(sizeof(FbBagAttrMeta) * state->tupdesc->natts);
	state->use_typed_identity = true;
	for (i = 0; i < state->tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(state->tupdesc, i);
		FbBagAttrMeta *meta;
		FbBagFastKind fast_kind;

		if (attr->attisdropped)
			continue;

		meta = &state->attr_meta[state->attr_count++];
		meta->attnum = attr->attnum;
		meta->attlen = attr->attlen;
		meta->attbyval = attr->attbyval;
		meta->attcollation = attr->attcollation;
		meta->fast_kind = fast_kind = fb_bag_fast_kind_for_type(attr->atttypid);
		if (attr->attnum > state->max_attnum)
			state->max_attnum = attr->attnum;

		if (fast_kind == FB_BAG_FAST_GENERIC)
		{
			TypeCacheEntry *typentry;

			typentry = lookup_type_cache(attr->atttypid,
										 TYPECACHE_HASH_PROC_FINFO |
										 TYPECACHE_EQ_OPR_FINFO);
			if (typentry == NULL ||
				typentry->hash_proc == InvalidOid ||
				typentry->eq_opr == InvalidOid)
			{
				state->use_typed_identity = false;
				break;
			}

			meta->hash_fmgr = typentry->hash_proc_finfo;
			meta->eq_fmgr = typentry->eq_opr_finfo;
		}
	}

	if (state->use_typed_identity)
	{
		state->scratch_values = palloc0(sizeof(Datum) * state->attr_count);
		state->scratch_nulls = palloc0(sizeof(bool) * state->attr_count);
	}
}

static Size
fb_bag_datum_owned_bytes(const FbBagAttrMeta *meta,
						 Datum value,
						 bool isnull)
{
	if (meta == NULL || isnull || meta->attbyval)
		return 0;

	return datumGetSize(value, meta->attbyval, meta->attlen);
}

static uint32
fb_bag_fast_hash(const FbBagAttrMeta *meta, Datum value)
{
	switch (meta->fast_kind)
	{
		case FB_BAG_FAST_BOOL:
		{
			bool v = DatumGetBool(value);

			return DatumGetUInt32(hash_any((const unsigned char *) &v, sizeof(v)));
		}
		case FB_BAG_FAST_INT2:
		{
			int16 v = DatumGetInt16(value);

			return DatumGetUInt32(hash_any((const unsigned char *) &v, sizeof(v)));
		}
		case FB_BAG_FAST_INT4:
		{
			int32 v = DatumGetInt32(value);

			return DatumGetUInt32(hash_any((const unsigned char *) &v, sizeof(v)));
		}
		case FB_BAG_FAST_INT8:
		{
			int64 v = DatumGetInt64(value);

			return DatumGetUInt32(hash_any((const unsigned char *) &v, sizeof(v)));
		}
		case FB_BAG_FAST_OID:
		{
			Oid v = DatumGetObjectId(value);

			return DatumGetUInt32(hash_any((const unsigned char *) &v, sizeof(v)));
		}
		case FB_BAG_FAST_UUID:
		{
			const pg_uuid_t *v = DatumGetUUIDP(value);

			return DatumGetUInt32(hash_any(v->data, UUID_LEN));
		}
		case FB_BAG_FAST_GENERIC:
			break;
	}

	return DatumGetUInt32(FunctionCall1Coll((FmgrInfo *) &meta->hash_fmgr,
											 meta->attcollation,
											 value));
}

static bool
fb_bag_fast_equal(const FbBagAttrMeta *meta, Datum left, Datum right)
{
	switch (meta->fast_kind)
	{
		case FB_BAG_FAST_BOOL:
			return DatumGetBool(left) == DatumGetBool(right);
		case FB_BAG_FAST_INT2:
			return DatumGetInt16(left) == DatumGetInt16(right);
		case FB_BAG_FAST_INT4:
			return DatumGetInt32(left) == DatumGetInt32(right);
		case FB_BAG_FAST_INT8:
			return DatumGetInt64(left) == DatumGetInt64(right);
		case FB_BAG_FAST_OID:
			return DatumGetObjectId(left) == DatumGetObjectId(right);
		case FB_BAG_FAST_UUID:
			return memcmp(DatumGetUUIDP(left)->data,
						  DatumGetUUIDP(right)->data,
						  UUID_LEN) == 0;
		case FB_BAG_FAST_GENERIC:
			break;
	}

	return DatumGetBool(FunctionCall2Coll((FmgrInfo *) &meta->eq_fmgr,
										 meta->attcollation,
										 left,
										 right));
}

static void
fb_bag_extract_tuple_values(FbBagApplyState *state, HeapTuple tuple)
{
	int i;

	for (i = 0; i < state->attr_count; i++)
		state->scratch_values[i] = heap_getattr(tuple,
												state->attr_meta[i].attnum,
												state->tupdesc,
												&state->scratch_nulls[i]);
}

static void
fb_bag_extract_slot_values(FbBagApplyState *state, TupleTableSlot *slot)
{
	int i;

	if (state->max_attnum > 0)
		slot_getsomeattrs(slot, state->max_attnum);

	for (i = 0; i < state->attr_count; i++)
	{
		int attr_index = state->attr_meta[i].attnum - 1;

		state->scratch_values[i] = slot->tts_values[attr_index];
		state->scratch_nulls[i] = slot->tts_isnull[attr_index];
	}
}

static uint32
fb_bag_hash_values(FbBagApplyState *state)
{
	uint32 hash_value = 0;
	int i;

	for (i = 0; i < state->attr_count; i++)
	{
		uint32 part_hash;

		if (state->scratch_nulls[i])
			part_hash = hash_bytes_uint32((uint32) (0x9e3779b9U + i));
		else
			part_hash = fb_bag_fast_hash(&state->attr_meta[i],
										 state->scratch_values[i]);
		hash_value = hash_combine(hash_value, part_hash);
	}

	return hash_value;
}

static bool
fb_bag_values_equal(FbBagApplyState *state, const FbBagEntry *entry)
{
	int i;

	for (i = 0; i < state->attr_count; i++)
	{
		const FbBagAttrMeta *meta = &state->attr_meta[i];

		if (entry->nulls[i] != state->scratch_nulls[i])
			return false;
		if (state->scratch_nulls[i])
			continue;
		if (!fb_bag_fast_equal(meta, entry->values[i], state->scratch_values[i]))
			return false;
	}

	return true;
}

/*
 * fb_bag_find_string_entry
 *    Bag apply helper.
 */

static FbBagEntry *
fb_bag_find_string_entry(HTAB *hash, const char *identity)
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

static FbBagEntry *
fb_bag_find_typed_entry(FbBagApplyState *state, uint32 hash_value)
{
	FbBagBucket *bucket;
	FbBagEntry *entry;

	bucket = (FbBagBucket *) hash_search(state->buckets, &hash_value, HASH_FIND, NULL);
	if (bucket == NULL)
		return NULL;

	for (entry = (FbBagEntry *) bucket->head;
		 entry != NULL;
		 entry = entry->bucket_next)
	{
		if (entry->hash_value == hash_value &&
			fb_bag_values_equal(state, entry))
			return entry;
	}

	return NULL;
}

static FbBagEntry *
fb_bag_create_entry(FbBagApplyState *state,
					  FbBagBucket *bucket,
					  uint32 hash_value)
{
	FbBagEntry *entry;

	entry = palloc0(sizeof(*entry));
	fb_bag_charge_bytes(state, sizeof(*entry), "apply bag delta entry");
	entry->hash_value = hash_value;
	entry->bucket_next = (FbBagEntry *) bucket->head;
	bucket->head = entry;

	if (state->entries_tail == NULL)
		state->entries_head = entry;
	else
		state->entries_tail->all_next = entry;
	state->entries_tail = entry;

	return entry;
}

static FbBagEntry *
fb_bag_get_or_create_string_entry(FbBagApplyState *state, const char *identity)
{
	FbBagBucket *bucket;
	FbBagEntry *entry;
	uint32 hash_value;
	bool found;

	entry = fb_bag_find_string_entry(state->buckets, identity);
	if (entry != NULL)
		return entry;

	hash_value = fb_apply_hash_identity(identity);
	bucket = (FbBagBucket *) hash_search(state->buckets, &hash_value, HASH_ENTER, &found);
	if (!found)
		bucket->head = NULL;

	entry = fb_bag_create_entry(state, bucket, hash_value);
	entry->row_identity = pstrdup(identity);
	fb_bag_charge_bytes(state,
						fb_memory_cstring_bytes(entry->row_identity),
						"apply bag row identity");

	return entry;
}

static FbBagEntry *
fb_bag_get_or_create_typed_entry(FbBagApplyState *state, HeapTuple tuple)
{
	FbBagBucket *bucket;
	FbBagEntry *entry;
	uint32 hash_value;
	bool found;
	int i;

	fb_bag_extract_tuple_values(state, tuple);
	hash_value = fb_bag_hash_values(state);
	entry = fb_bag_find_typed_entry(state, hash_value);
	if (entry != NULL)
		return entry;

	bucket = (FbBagBucket *) hash_search(state->buckets, &hash_value, HASH_ENTER, &found);
	if (!found)
		bucket->head = NULL;

	entry = fb_bag_create_entry(state, bucket, hash_value);
	entry->values = palloc0(sizeof(Datum) * state->attr_count);
	entry->nulls = palloc0(sizeof(bool) * state->attr_count);
	fb_bag_charge_bytes(state,
						sizeof(Datum) * state->attr_count +
						sizeof(bool) * state->attr_count,
						"apply bag typed row identity");
	for (i = 0; i < state->attr_count; i++)
	{
		entry->nulls[i] = state->scratch_nulls[i];
		if (!entry->nulls[i])
		{
			entry->values[i] = datumCopy(state->scratch_values[i],
										 state->attr_meta[i].attbyval,
										 state->attr_meta[i].attlen);
			fb_bag_charge_bytes(state,
								fb_bag_datum_owned_bytes(&state->attr_meta[i],
														 state->scratch_values[i],
														 false),
								"apply bag typed row datum");
		}
	}

	return entry;
}

/*
 * fb_bag_adjust_delta
 *    Bag apply helper.
 */

static void
fb_bag_adjust_delta(FbBagApplyState *state,
					 HeapTuple tuple,
					 int64 delta)
{
	FbBagEntry *entry;

	if (tuple == NULL)
		return;

	if (state->use_typed_identity)
		entry = fb_bag_get_or_create_typed_entry(state, tuple);
	else
	{
		char *identity = fb_apply_build_row_identity(tuple, state->tupdesc);

		entry = fb_bag_get_or_create_string_entry(state, identity);
		pfree(identity);
	}

	entry->delta += delta;
	if (delta > 0 && entry->tuple == NULL)
	{
		entry->tuple = heap_copytuple(tuple);
		fb_bag_charge_bytes(state,
							fb_memory_heaptuple_bytes(entry->tuple),
							"apply bag residual tuple");
	}
}

/*
 * fb_bag_apply_begin
 *    Bag apply entry point.
 */

void *
fb_bag_apply_begin(const FbRelationInfo *info,
				   TupleDesc tupdesc,
				   const FbReverseOpSource *source)
{
	FbBagApplyState *state;
	FbReverseOpReader *reader;
	FbReverseOp op;

	(void) info;

	state = palloc0(sizeof(*state));
	state->tupdesc = tupdesc;
	state->buckets = fb_bag_create_bucket_hash(fb_bag_bucket_target(source));
	state->tracked_bytes = fb_reverse_source_tracked_bytes(source);
	state->memory_limit_bytes = fb_reverse_source_memory_limit_bytes(source);
	fb_bag_init_attr_meta(state);

	reader = fb_reverse_reader_open(source);
	while (fb_reverse_reader_next(reader, &op))
	{
		switch (op.type)
		{
			case FB_REVERSE_REMOVE:
				fb_bag_adjust_delta(state, op.new_row.tuple, -1);
				break;
			case FB_REVERSE_ADD:
				fb_bag_adjust_delta(state, op.old_row.tuple, 1);
				break;
			case FB_REVERSE_REPLACE:
				fb_bag_adjust_delta(state, op.new_row.tuple, -1);
				fb_bag_adjust_delta(state, op.old_row.tuple, 1);
				break;
		}
	}
	fb_reverse_reader_close(reader);

	return state;
}

static bool
fb_bag_emit_current(FbBagEntry *entry,
					TupleTableSlot *slot,
					FbApplyEmit *emit)
{
	if (emit == NULL)
		return false;

	emit->kind = FB_APPLY_EMIT_NONE;
	emit->slot = NULL;
	emit->tuple = NULL;

	if (entry == NULL)
	{
		emit->kind = FB_APPLY_EMIT_SLOT;
		emit->slot = slot;
		return true;
	}

	if (entry->delta < 0)
	{
		entry->delta++;
		return false;
	}

	emit->kind = FB_APPLY_EMIT_SLOT;
	emit->slot = slot;
	return true;
}

static bool
fb_bag_emit_residual(FbBagApplyState *state, FbApplyEmit *emit)
{
	if (emit == NULL)
		return false;

	emit->kind = FB_APPLY_EMIT_NONE;
	emit->slot = NULL;
	emit->tuple = NULL;

	while (state->residual_cursor != NULL)
	{
		if (state->residual_repeat == 0)
		{
			if (state->residual_cursor->delta <= 0)
			{
				state->residual_cursor = state->residual_cursor->all_next;
				continue;
			}

			state->residual_repeat = state->residual_cursor->delta;
		}

		state->residual_repeat--;
		emit->kind = FB_APPLY_EMIT_TUPLE;
		emit->tuple = state->residual_cursor->tuple;
		if (state->residual_repeat == 0)
			state->residual_cursor = state->residual_cursor->all_next;
		return true;
	}

	return false;
}

/*
 * fb_bag_apply_process_current
 *    Bag apply entry point.
 */

bool
fb_bag_apply_process_current(void *arg,
							   TupleTableSlot *slot,
							   Datum *result)
{
	FbBagApplyState *state = (FbBagApplyState *) arg;
	FbBagEntry *entry;
	FbApplyEmit emit;

	if (state->use_typed_identity)
	{
		uint32 hash_value;

		fb_bag_extract_slot_values(state, slot);
		hash_value = fb_bag_hash_values(state);
		entry = fb_bag_find_typed_entry(state, hash_value);
	}
	else
	{
		char *identity;

		identity = fb_apply_build_row_identity_slot(slot, state->tupdesc);
		entry = fb_bag_find_string_entry(state->buckets, identity);
		pfree(identity);
	}

	if (!fb_bag_emit_current(entry, slot, &emit))
		return false;

	*result = ExecFetchSlotHeapTupleDatum(emit.slot);
	return true;
}

bool
fb_bag_apply_process_current_emit(void *arg,
								  TupleTableSlot *slot,
								  FbApplyEmit *emit)
{
	FbBagApplyState *state = (FbBagApplyState *) arg;
	FbBagEntry *entry;

	if (state->use_typed_identity)
	{
		uint32 hash_value;

		fb_bag_extract_slot_values(state, slot);
		hash_value = fb_bag_hash_values(state);
		entry = fb_bag_find_typed_entry(state, hash_value);
	}
	else
	{
		char *identity;

		identity = fb_apply_build_row_identity_slot(slot, state->tupdesc);
		entry = fb_bag_find_string_entry(state->buckets, identity);
		pfree(identity);
	}

	return fb_bag_emit_current(entry, slot, emit);
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

bool
fb_bag_apply_next_residual(void *arg, Datum *result)
{
	FbBagApplyState *state = (FbBagApplyState *) arg;
	FbApplyEmit emit;

	if (!fb_bag_emit_residual(state, &emit))
		return false;

	*result = heap_copy_tuple_as_datum(emit.tuple, state->tupdesc);
	return true;
}

bool
fb_bag_apply_next_residual_emit(void *arg, FbApplyEmit *emit)
{
	FbBagApplyState *state = (FbBagApplyState *) arg;

	return fb_bag_emit_residual(state, emit);
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
