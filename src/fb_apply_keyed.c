/*
 * fb_apply_keyed.c
 *    Keyed streaming reverse-op application helpers.
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

typedef enum FbKeyedFastKind
{
	FB_KEYED_FAST_GENERIC = 0,
	FB_KEYED_FAST_BOOL,
	FB_KEYED_FAST_INT2,
	FB_KEYED_FAST_INT4,
	FB_KEYED_FAST_INT8,
	FB_KEYED_FAST_OID,
	FB_KEYED_FAST_UUID
} FbKeyedFastKind;

typedef struct FbKeyedBucket
{
	uint32 hash;
	void *head;
} FbKeyedBucket;

typedef struct FbKeyedAttrMeta
{
	AttrNumber attnum;
	int16 attlen;
	bool attbyval;
	Oid attcollation;
	FbKeyedFastKind fast_kind;
	FmgrInfo hash_fmgr;
	FmgrInfo eq_fmgr;
} FbKeyedAttrMeta;

typedef struct FbKeyedEntry
{
	char *key_identity;
	Datum key_values[INDEX_MAX_KEYS];
	bool key_nulls[INDEX_MAX_KEYS];
	uint32 hash_value;
	bool current_seen;
	HeapTuple replacement_tuple;
	struct FbKeyedEntry *bucket_next;
	struct FbKeyedEntry *all_next;
} FbKeyedEntry;

typedef struct FbKeyedApplyState
{
	const FbRelationInfo *info;
	TupleDesc tupdesc;
	HTAB *buckets;
	FbKeyedEntry *entries_head;
	FbKeyedEntry *entries_tail;
	FbKeyedEntry *residual_cursor;
	uint64 residual_total;
	uint64 tracked_bytes;
	uint64 memory_limit_bytes;
	bool use_typed_keys;
	int key_natts;
	int max_key_attnum;
	FbKeyedAttrMeta key_meta[INDEX_MAX_KEYS];
} FbKeyedApplyState;

/*
 * fb_keyed_charge_bytes
 *    Keyed apply helper.
 */

static void
fb_keyed_charge_bytes(FbKeyedApplyState *state, Size bytes, const char *what)
{
	if (state == NULL)
		return;

	fb_memory_charge_bytes(&state->tracked_bytes,
						   state->memory_limit_bytes,
						   bytes,
						   what);
}

/*
 * fb_apply_hash_identity
 *    Apply entry point.
 */

uint32
fb_apply_hash_identity(const char *identity)
{
	if (identity == NULL)
		return 0;

	return DatumGetUInt32(hash_any((const unsigned char *) identity,
								   strlen(identity)));
}

/*
 * fb_apply_tuple_identity
 *    Keyed apply helper.
 */

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

/*
 * fb_apply_build_key_identity
 *    Apply entry point.
 */

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

/*
 * fb_apply_create_bucket_hash
 *    Keyed apply helper.
 */

static HTAB *
fb_apply_create_bucket_hash(const char *name, long initial_size)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(FbKeyedBucket);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create(name, Max(initial_size, 128L), &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static long
fb_keyed_bucket_target(const FbReverseOpSource *source)
{
	uint64 total = fb_reverse_source_total_count(source);

	if (total < 128)
		return 128;
	if (total > (uint64) LONG_MAX)
		return LONG_MAX;
	return (long) total;
}

static FbKeyedFastKind
fb_keyed_fast_kind_for_type(Oid type_oid)
{
	switch (type_oid)
	{
		case BOOLOID:
			return FB_KEYED_FAST_BOOL;
		case INT2OID:
			return FB_KEYED_FAST_INT2;
		case INT4OID:
			return FB_KEYED_FAST_INT4;
		case INT8OID:
			return FB_KEYED_FAST_INT8;
		case OIDOID:
			return FB_KEYED_FAST_OID;
		case UUIDOID:
			return FB_KEYED_FAST_UUID;
		default:
			return FB_KEYED_FAST_GENERIC;
	}
}

static bool
fb_keyed_emit_current(FbKeyedEntry *entry,
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

	if (entry->current_seen)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("fb keyed apply saw duplicate current row for one key")));

	entry->current_seen = true;
	if (entry->replacement_tuple == NULL)
		return false;

	emit->kind = FB_APPLY_EMIT_TUPLE;
	emit->tuple = entry->replacement_tuple;
	return true;
}

static bool
fb_keyed_emit_residual(FbKeyedApplyState *state, FbApplyEmit *emit)
{
	if (emit == NULL)
		return false;

	emit->kind = FB_APPLY_EMIT_NONE;
	emit->slot = NULL;
	emit->tuple = NULL;

	while (state->residual_cursor != NULL)
	{
		FbKeyedEntry *entry = state->residual_cursor;

		state->residual_cursor = entry->all_next;
		if (entry->current_seen)
			continue;
		if (entry->replacement_tuple == NULL)
			continue;

		emit->kind = FB_APPLY_EMIT_TUPLE;
		emit->tuple = entry->replacement_tuple;
		return true;
	}

	return false;
}

static void
fb_keyed_init_key_meta(FbKeyedApplyState *state)
{
	int i;

	state->use_typed_keys = false;
	state->key_natts = (state->info != NULL) ? state->info->key_natts : 0;
	state->max_key_attnum = 0;
	if (state->key_natts <= 0 || state->tupdesc == NULL)
		return;

	state->use_typed_keys = true;
	for (i = 0; i < state->key_natts; i++)
	{
		AttrNumber attnum = state->info->key_attnums[i];
		Form_pg_attribute attr;
		FbKeyedFastKind fast_kind;

		if (attnum <= 0 || attnum > state->tupdesc->natts)
		{
			state->use_typed_keys = false;
			break;
		}

		attr = TupleDescAttr(state->tupdesc, attnum - 1);
		if (attr->attisdropped)
		{
			state->use_typed_keys = false;
			break;
		}

		fast_kind = fb_keyed_fast_kind_for_type(attr->atttypid);
		state->key_meta[i].attnum = attnum;
		state->key_meta[i].attlen = attr->attlen;
		state->key_meta[i].attbyval = attr->attbyval;
		state->key_meta[i].attcollation = attr->attcollation;
		state->key_meta[i].fast_kind = fast_kind;
		if (attnum > state->max_key_attnum)
			state->max_key_attnum = attnum;

		if (fast_kind == FB_KEYED_FAST_GENERIC)
		{
			TypeCacheEntry *typentry;

			typentry = lookup_type_cache(attr->atttypid,
										 TYPECACHE_HASH_PROC_FINFO |
										 TYPECACHE_EQ_OPR_FINFO);
			if (typentry == NULL ||
				typentry->hash_proc == InvalidOid ||
				typentry->eq_opr == InvalidOid)
			{
				state->use_typed_keys = false;
				break;
			}

			state->key_meta[i].hash_fmgr = typentry->hash_proc_finfo;
			state->key_meta[i].eq_fmgr = typentry->eq_opr_finfo;
		}
	}
}

static Size
fb_keyed_datum_owned_bytes(const FbKeyedAttrMeta *meta,
						   Datum value,
						   bool isnull)
{
	if (meta == NULL || isnull || meta->attbyval)
		return 0;

	return datumGetSize(value, meta->attbyval, meta->attlen);
}

static uint32
fb_keyed_fast_hash(const FbKeyedAttrMeta *meta, Datum value)
{
	switch (meta->fast_kind)
	{
		case FB_KEYED_FAST_BOOL:
		{
			bool v = DatumGetBool(value);

			return DatumGetUInt32(hash_any((const unsigned char *) &v, sizeof(v)));
		}
		case FB_KEYED_FAST_INT2:
		{
			int16 v = DatumGetInt16(value);

			return DatumGetUInt32(hash_any((const unsigned char *) &v, sizeof(v)));
		}
		case FB_KEYED_FAST_INT4:
		{
			int32 v = DatumGetInt32(value);

			return DatumGetUInt32(hash_any((const unsigned char *) &v, sizeof(v)));
		}
		case FB_KEYED_FAST_INT8:
		{
			int64 v = DatumGetInt64(value);

			return DatumGetUInt32(hash_any((const unsigned char *) &v, sizeof(v)));
		}
		case FB_KEYED_FAST_OID:
		{
			Oid v = DatumGetObjectId(value);

			return DatumGetUInt32(hash_any((const unsigned char *) &v, sizeof(v)));
		}
		case FB_KEYED_FAST_UUID:
		{
			const pg_uuid_t *v = DatumGetUUIDP(value);

			return DatumGetUInt32(hash_any(v->data, UUID_LEN));
		}
		case FB_KEYED_FAST_GENERIC:
			break;
	}

	return DatumGetUInt32(FunctionCall1Coll((FmgrInfo *) &meta->hash_fmgr,
											 meta->attcollation,
											 value));
}

static bool
fb_keyed_fast_equal(const FbKeyedAttrMeta *meta, Datum left, Datum right)
{
	switch (meta->fast_kind)
	{
		case FB_KEYED_FAST_BOOL:
			return DatumGetBool(left) == DatumGetBool(right);
		case FB_KEYED_FAST_INT2:
			return DatumGetInt16(left) == DatumGetInt16(right);
		case FB_KEYED_FAST_INT4:
			return DatumGetInt32(left) == DatumGetInt32(right);
		case FB_KEYED_FAST_INT8:
			return DatumGetInt64(left) == DatumGetInt64(right);
		case FB_KEYED_FAST_OID:
			return DatumGetObjectId(left) == DatumGetObjectId(right);
		case FB_KEYED_FAST_UUID:
			return memcmp(DatumGetUUIDP(left)->data,
						  DatumGetUUIDP(right)->data,
						  UUID_LEN) == 0;
		case FB_KEYED_FAST_GENERIC:
			break;
	}

	return DatumGetBool(FunctionCall2Coll((FmgrInfo *) &meta->eq_fmgr,
										 meta->attcollation,
										 left,
										 right));
}

static void
fb_keyed_extract_tuple_values(FbKeyedApplyState *state,
							  HeapTuple tuple,
							  Datum *values,
							  bool *nulls)
{
	int i;

	for (i = 0; i < state->key_natts; i++)
		values[i] = heap_getattr(tuple,
								 state->key_meta[i].attnum,
								 state->tupdesc,
								 &nulls[i]);
}

static void
fb_keyed_extract_slot_values(FbKeyedApplyState *state,
							 TupleTableSlot *slot,
							 Datum *values,
							 bool *nulls)
{
	int i;

	if (state->max_key_attnum > 0)
		slot_getsomeattrs(slot, state->max_key_attnum);

	for (i = 0; i < state->key_natts; i++)
	{
		int attr_index = state->key_meta[i].attnum - 1;

		values[i] = slot->tts_values[attr_index];
		nulls[i] = slot->tts_isnull[attr_index];
	}
}

static uint32
fb_keyed_hash_values(FbKeyedApplyState *state,
					 const Datum *values,
					 const bool *nulls)
{
	uint32 hash_value = 0;
	int i;

	for (i = 0; i < state->key_natts; i++)
	{
		uint32 part_hash;

		if (nulls[i])
			part_hash = hash_bytes_uint32((uint32) (0x9e3779b9U + i));
		else
			part_hash = fb_keyed_fast_hash(&state->key_meta[i], values[i]);
		hash_value = hash_combine(hash_value, part_hash);
	}

	return hash_value;
}

static bool
fb_keyed_values_equal(FbKeyedApplyState *state,
					  const FbKeyedEntry *entry,
					  const Datum *values,
					  const bool *nulls)
{
	int i;

	for (i = 0; i < state->key_natts; i++)
	{
		const FbKeyedAttrMeta *meta = &state->key_meta[i];

		if (entry->key_nulls[i] != nulls[i])
			return false;
		if (nulls[i])
			continue;
		if (!fb_keyed_fast_equal(meta, entry->key_values[i], values[i]))
			return false;
	}

	return true;
}

static FbKeyedEntry *
fb_keyed_find_typed_entry(FbKeyedApplyState *state,
						  uint32 hash_value,
						  const Datum *values,
						  const bool *nulls)
{
	FbKeyedBucket *bucket;
	FbKeyedEntry *entry;

	bucket = (FbKeyedBucket *) hash_search(state->buckets,
										   &hash_value,
										   HASH_FIND,
										   NULL);
	if (bucket == NULL)
		return NULL;

	for (entry = (FbKeyedEntry *) bucket->head;
		 entry != NULL;
		 entry = entry->bucket_next)
	{
		if (entry->hash_value == hash_value &&
			fb_keyed_values_equal(state, entry, values, nulls))
			return entry;
	}

	return NULL;
}

/*
 * fb_keyed_find_string_entry
 *    Keyed apply helper.
 */

static FbKeyedEntry *
fb_keyed_find_string_entry(HTAB *hash, const char *identity)
{
	FbKeyedBucket *bucket;
	FbKeyedEntry *entry;
	uint32 hash_value;

	hash_value = fb_apply_hash_identity(identity);
	bucket = (FbKeyedBucket *) hash_search(hash, &hash_value, HASH_FIND, NULL);
	if (bucket == NULL)
		return NULL;

	for (entry = (FbKeyedEntry *) bucket->head;
		 entry != NULL;
		 entry = entry->bucket_next)
	{
		if (strcmp(entry->key_identity, identity) == 0)
			return entry;
	}

	return NULL;
}

static FbKeyedEntry *
fb_keyed_create_entry(FbKeyedApplyState *state,
					  uint32 hash_value)
{
	FbKeyedEntry *entry;
	FbKeyedBucket *bucket;
	bool found;

	entry = palloc0(sizeof(*entry));
	fb_keyed_charge_bytes(state, sizeof(*entry), "apply keyed changed-key entry");
	entry->hash_value = hash_value;
	bucket = (FbKeyedBucket *) hash_search(state->buckets, &hash_value, HASH_ENTER, &found);
	if (!found)
		bucket->head = NULL;
	entry->bucket_next = (FbKeyedEntry *) bucket->head;
	bucket->head = entry;

	if (state->entries_tail == NULL)
		state->entries_head = entry;
	else
		state->entries_tail->all_next = entry;
	state->entries_tail = entry;

	return entry;
}

static FbKeyedEntry *
fb_keyed_get_or_create_typed_entry(FbKeyedApplyState *state,
								   HeapTuple tuple)
{
	FbKeyedEntry *entry;
	Datum values[INDEX_MAX_KEYS];
	bool nulls[INDEX_MAX_KEYS];
	uint32 hash_value;
	int i;

	if (tuple == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("fb keyed apply is missing tuple for typed-key state")));

	fb_keyed_extract_tuple_values(state, tuple, values, nulls);
	hash_value = fb_keyed_hash_values(state, values, nulls);
	entry = fb_keyed_find_typed_entry(state, hash_value, values, nulls);
	if (entry != NULL)
		return entry;

	entry = fb_keyed_create_entry(state, hash_value);
	for (i = 0; i < state->key_natts; i++)
	{
		entry->key_nulls[i] = nulls[i];
		if (nulls[i])
			entry->key_values[i] = (Datum) 0;
		else
		{
			entry->key_values[i] = datumCopy(values[i],
											 state->key_meta[i].attbyval,
											 state->key_meta[i].attlen);
			fb_keyed_charge_bytes(state,
								  fb_keyed_datum_owned_bytes(&state->key_meta[i],
															 values[i],
															 false),
								  "apply keyed typed key datum");
		}
	}

	return entry;
}

/*
 * fb_keyed_get_or_create_string_entry
 *    Keyed apply helper.
 */

static FbKeyedEntry *
fb_keyed_get_or_create_string_entry(FbKeyedApplyState *state, const char *identity)
{
	FbKeyedEntry *entry;
	uint32 hash_value;

	entry = fb_keyed_find_string_entry(state->buckets, identity);
	if (entry != NULL)
		return entry;

	hash_value = fb_apply_hash_identity(identity);
	entry = fb_keyed_create_entry(state, hash_value);
	entry->key_identity = pstrdup(identity);
	fb_keyed_charge_bytes(state,
						  fb_memory_cstring_bytes(entry->key_identity),
						  "apply keyed changed-key identity");

	return entry;
}

/*
 * fb_keyed_replace_tuple
 *    Keyed apply helper.
 */

static void
fb_keyed_replace_tuple(FbKeyedApplyState *state,
					   FbKeyedEntry *entry,
					   HeapTuple tuple)
{
	if (state == NULL || entry == NULL)
		return;

	if (entry->replacement_tuple != NULL)
	{
		fb_memory_release_bytes(&state->tracked_bytes,
								fb_memory_heaptuple_bytes(entry->replacement_tuple));
		heap_freetuple(entry->replacement_tuple);
		entry->replacement_tuple = NULL;
	}

	if (tuple != NULL)
	{
		entry->replacement_tuple = heap_copytuple(tuple);
		fb_keyed_charge_bytes(state,
							  fb_memory_heaptuple_bytes(entry->replacement_tuple),
							  "apply keyed replacement tuple");
	}
}

/*
 * fb_keyed_record_state
 *    Keyed apply helper.
 */

static void
fb_keyed_record_state(FbKeyedApplyState *state,
					  HeapTuple key_tuple,
					  HeapTuple replacement_tuple)
{
	FbKeyedEntry *entry;

	if (key_tuple == NULL)
		return;

	if (state->use_typed_keys)
		entry = fb_keyed_get_or_create_typed_entry(state, key_tuple);
	else
	{
		char *identity = fb_apply_build_key_identity(state->info, key_tuple, state->tupdesc);

		entry = fb_keyed_get_or_create_string_entry(state, identity);
		pfree(identity);
	}

	fb_keyed_replace_tuple(state, entry, replacement_tuple);
}

/*
 * fb_keyed_apply_begin
 *    Keyed apply entry point.
 */

void *
fb_keyed_apply_begin(const FbRelationInfo *info,
					 TupleDesc tupdesc,
					 const FbReverseOpSource *source)
{
	FbKeyedApplyState *state;
	FbReverseOpReader *reader;
	FbReverseOp op;

	state = palloc0(sizeof(*state));
	state->info = info;
	state->tupdesc = tupdesc;
	state->buckets = fb_apply_create_bucket_hash("fb keyed changed keys",
												 fb_keyed_bucket_target(source));
	state->tracked_bytes = fb_reverse_source_tracked_bytes(source);
	state->memory_limit_bytes = fb_reverse_source_memory_limit_bytes(source);
	fb_keyed_init_key_meta(state);

	reader = fb_reverse_reader_open(source);
	while (fb_reverse_reader_next(reader, &op))
	{
		switch (op.type)
		{
			case FB_REVERSE_REMOVE:
				fb_keyed_record_state(state, op.new_row.tuple, NULL);
				break;
			case FB_REVERSE_ADD:
				fb_keyed_record_state(state, op.old_row.tuple, op.old_row.tuple);
				break;
			case FB_REVERSE_REPLACE:
				fb_keyed_record_state(state, op.new_row.tuple, NULL);
				fb_keyed_record_state(state, op.old_row.tuple, op.old_row.tuple);
				break;
		}
	}
	fb_reverse_reader_close(reader);

	return state;
}

/*
 * fb_keyed_apply_process_current
 *    Keyed apply entry point.
 */

bool
fb_keyed_apply_process_current(void *arg,
								 TupleTableSlot *slot,
								 Datum *result)
{
	FbKeyedApplyState *state = (FbKeyedApplyState *) arg;
	FbKeyedEntry *entry;
	FbApplyEmit emit;

	if (state->use_typed_keys)
	{
		Datum values[INDEX_MAX_KEYS];
		bool nulls[INDEX_MAX_KEYS];
		uint32 hash_value;

		fb_keyed_extract_slot_values(state, slot, values, nulls);
		hash_value = fb_keyed_hash_values(state, values, nulls);
		entry = fb_keyed_find_typed_entry(state, hash_value, values, nulls);
	}
	else
	{
		char *identity;

		identity = fb_apply_build_key_identity_slot(state->info, slot, state->tupdesc);
		entry = fb_keyed_find_string_entry(state->buckets, identity);
		pfree(identity);
	}

	if (!fb_keyed_emit_current(entry, slot, &emit))
		return false;

	if (emit.kind == FB_APPLY_EMIT_SLOT)
		*result = ExecFetchSlotHeapTupleDatum(emit.slot);
	else
		*result = heap_copy_tuple_as_datum(emit.tuple, state->tupdesc);

	return true;
}

bool
fb_keyed_apply_process_current_emit(void *arg,
									TupleTableSlot *slot,
									FbApplyEmit *emit)
{
	FbKeyedApplyState *state = (FbKeyedApplyState *) arg;
	FbKeyedEntry *entry;

	if (state->use_typed_keys)
	{
		Datum values[INDEX_MAX_KEYS];
		bool nulls[INDEX_MAX_KEYS];
		uint32 hash_value;

		fb_keyed_extract_slot_values(state, slot, values, nulls);
		hash_value = fb_keyed_hash_values(state, values, nulls);
		entry = fb_keyed_find_typed_entry(state, hash_value, values, nulls);
	}
	else
	{
		char *identity;

		identity = fb_apply_build_key_identity_slot(state->info, slot, state->tupdesc);
		entry = fb_keyed_find_string_entry(state->buckets, identity);
		pfree(identity);
	}

	return fb_keyed_emit_current(entry, slot, emit);
}

/*
 * fb_keyed_apply_finish_scan
 *    Keyed apply entry point.
 */

void
fb_keyed_apply_finish_scan(void *arg)
{
	FbKeyedApplyState *state = (FbKeyedApplyState *) arg;
	FbKeyedEntry *entry;

	state->residual_total = 0;
	for (entry = state->entries_head; entry != NULL; entry = entry->all_next)
	{
		if (!entry->current_seen && entry->replacement_tuple != NULL)
			state->residual_total++;
	}

	state->residual_cursor = state->entries_head;
}

/*
 * fb_keyed_apply_residual_total
 *    Keyed apply entry point.
 */

uint64
fb_keyed_apply_residual_total(void *arg)
{
	FbKeyedApplyState *state = (FbKeyedApplyState *) arg;

	return state->residual_total;
}

/*
 * fb_keyed_apply_next_residual
 *    Keyed apply entry point.
 */

bool
fb_keyed_apply_next_residual(void *arg, Datum *result)
{
	FbKeyedApplyState *state = (FbKeyedApplyState *) arg;
	FbApplyEmit emit;

	if (!fb_keyed_emit_residual(state, &emit))
		return false;

	*result = heap_copy_tuple_as_datum(emit.tuple, state->tupdesc);
	return true;
}

bool
fb_keyed_apply_next_residual_emit(void *arg, FbApplyEmit *emit)
{
	FbKeyedApplyState *state = (FbKeyedApplyState *) arg;

	return fb_keyed_emit_residual(state, emit);
}

/*
 * fb_keyed_apply_end
 *    Keyed apply entry point.
 */

void
fb_keyed_apply_end(void *arg)
{
	(void) arg;
}
