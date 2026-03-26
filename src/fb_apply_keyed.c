/*
 * fb_apply_keyed.c
 *    Keyed streaming reverse-op application helpers.
 */

#include "postgres.h"

#include "access/hash.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"

#include "fb_apply.h"
#include "fb_memory.h"

typedef enum FbKeyedActionType
{
	FB_KEYED_ACTION_REMOVE = 1,
	FB_KEYED_ACTION_ADD
} FbKeyedActionType;

typedef struct FbKeyedBucket
{
	uint32 hash;
	void *head;
} FbKeyedBucket;

typedef struct FbKeyedAction
{
	FbKeyedActionType type;
	HeapTuple tuple;
	struct FbKeyedAction *next;
} FbKeyedAction;

typedef struct FbKeyedEntry
{
	char *key_identity;
	bool current_seen;
	FbKeyedAction *actions_head;
	FbKeyedAction *actions_tail;
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
 * fb_apply_create_bucket_hash
 *    Keyed apply helper.
 */

static HTAB *
fb_apply_create_bucket_hash(const char *name)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(FbKeyedBucket);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create(name, 128, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
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
 * fb_keyed_find_entry
 *    Keyed apply helper.
 */

static FbKeyedEntry *
fb_keyed_find_entry(HTAB *hash, const char *identity)
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

/*
 * fb_keyed_get_or_create_entry
 *    Keyed apply helper.
 */

static FbKeyedEntry *
fb_keyed_get_or_create_entry(FbKeyedApplyState *state, const char *identity)
{
	FbKeyedBucket *bucket;
	FbKeyedEntry *entry;
	uint32 hash_value;
	bool found;

	entry = fb_keyed_find_entry(state->buckets, identity);
	if (entry != NULL)
		return entry;

	hash_value = fb_apply_hash_identity(identity);
	bucket = (FbKeyedBucket *) hash_search(state->buckets, &hash_value, HASH_ENTER, &found);
	if (!found)
		bucket->head = NULL;

	entry = palloc0(sizeof(*entry));
	fb_keyed_charge_bytes(state, sizeof(*entry), "apply keyed changed-key entry");
	entry->key_identity = pstrdup(identity);
	fb_keyed_charge_bytes(state,
						  fb_memory_cstring_bytes(entry->key_identity),
						  "apply keyed changed-key identity");
	entry->bucket_next = (FbKeyedEntry *) bucket->head;
	bucket->head = entry;

	if (state->entries_tail == NULL)
		state->entries_head = entry;
	else
		state->entries_tail->all_next = entry;
	state->entries_tail = entry;

	return entry;
}

/*
 * fb_keyed_append_action
 *    Keyed apply helper.
 */

static void
fb_keyed_append_action(FbKeyedApplyState *state,
					   const char *identity,
					   FbKeyedActionType type,
					   HeapTuple tuple)
{
	FbKeyedEntry *entry;
	FbKeyedAction *action;

	if (identity == NULL)
		return;

	entry = fb_keyed_get_or_create_entry(state, identity);
	action = palloc0(sizeof(*action));
	fb_keyed_charge_bytes(state, sizeof(*action), "apply keyed action");
	action->type = type;
	action->tuple = tuple;

	if (entry->actions_tail == NULL)
		entry->actions_head = action;
	else
		entry->actions_tail->next = action;
	entry->actions_tail = action;
}

/*
 * fb_keyed_apply_actions
 *    Keyed apply helper.
 */

static HeapTuple
fb_keyed_apply_actions(FbKeyedEntry *entry, HeapTuple current_tuple)
{
	HeapTuple state = current_tuple;
	FbKeyedAction *action;

	for (action = entry->actions_head; action != NULL; action = action->next)
	{
		if (action->type == FB_KEYED_ACTION_REMOVE)
			state = NULL;
		else
			state = action->tuple;
	}

	return state;
}

/*
 * fb_keyed_apply_begin
 *    Keyed apply entry point.
 */

void *
fb_keyed_apply_begin(const FbRelationInfo *info,
					 TupleDesc tupdesc,
					 const FbReverseOpStream *stream)
{
	FbKeyedApplyState *state;
	uint32 i;

	state = palloc0(sizeof(*state));
	state->info = info;
	state->tupdesc = tupdesc;
	state->buckets = fb_apply_create_bucket_hash("fb keyed changed keys");
	state->tracked_bytes = stream->tracked_bytes;
	state->memory_limit_bytes = stream->memory_limit_bytes;

	for (i = 0; i < stream->count; i++)
	{
		const FbReverseOp *op = &stream->ops[i];

		switch (op->type)
		{
			case FB_REVERSE_REMOVE:
				fb_keyed_append_action(state,
									   op->new_row.key_identity,
									   FB_KEYED_ACTION_REMOVE,
									   NULL);
				break;
			case FB_REVERSE_ADD:
				fb_keyed_append_action(state,
									   op->old_row.key_identity,
									   FB_KEYED_ACTION_ADD,
									   op->old_row.tuple);
				break;
			case FB_REVERSE_REPLACE:
				fb_keyed_append_action(state,
									   op->new_row.key_identity,
									   FB_KEYED_ACTION_REMOVE,
									   NULL);
				fb_keyed_append_action(state,
									   op->old_row.key_identity,
									   FB_KEYED_ACTION_ADD,
									   op->old_row.tuple);
				break;
		}
	}

	return state;
}

/*
 * fb_keyed_apply_process_current
 *    Keyed apply entry point.
 */

HeapTuple
fb_keyed_apply_process_current(void *arg, HeapTuple tuple)
{
	FbKeyedApplyState *state = (FbKeyedApplyState *) arg;
	FbKeyedEntry *entry;
	char *identity;
	HeapTuple result;

	identity = fb_apply_build_key_identity(state->info, tuple, state->tupdesc);
	entry = fb_keyed_find_entry(state->buckets, identity);
	pfree(identity);

	if (entry == NULL)
		return tuple;

	if (entry->current_seen)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("fb keyed apply saw duplicate current row for one key")));

	entry->current_seen = true;
	result = fb_keyed_apply_actions(entry, tuple);
	if (result != tuple)
		heap_freetuple(tuple);
	return result;
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
		if (!entry->current_seen && fb_keyed_apply_actions(entry, NULL) != NULL)
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

HeapTuple
fb_keyed_apply_next_residual(void *arg)
{
	FbKeyedApplyState *state = (FbKeyedApplyState *) arg;

	while (state->residual_cursor != NULL)
	{
		FbKeyedEntry *entry = state->residual_cursor;
		HeapTuple result;

		state->residual_cursor = entry->all_next;
		if (entry->current_seen)
			continue;

		result = fb_keyed_apply_actions(entry, NULL);
		if (result != NULL)
			return result;
	}

	return NULL;
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
