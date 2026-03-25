#include "postgres.h"

#include <unistd.h>

#include "access/detoast.h"
#include "access/genam.h"
#include "access/sdir.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/toast_internals.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/wait_event_types.h"
#include "utils/hsearch.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

#include "fb_runtime.h"
#include "fb_toast.h"

typedef struct FbToastChunkKey
{
	Oid valueid;
	int32 chunk_seq;
} FbToastChunkKey;

typedef struct FbToastChunkEntry
{
	FbToastChunkKey key;
	int32 len;
	char *data;
	bool spilled;
	off_t spill_offset;
} FbToastChunkEntry;

struct FbToastStore
{
	HTAB *live_chunks;
	HTAB *retired_chunks;
	File spill_file;
	char *spill_path;
	off_t spill_size;
};

static uint64 fb_toast_spill_counter = 0;

static HTAB *
fb_toast_create_chunk_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(FbToastChunkKey);
	ctl.entrysize = sizeof(FbToastChunkEntry);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create("fb toast chunks", 256, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

FbToastStore *
fb_toast_store_create(void)
{
	FbToastStore *store = palloc0(sizeof(FbToastStore));

	store->live_chunks = fb_toast_create_chunk_hash();
	store->retired_chunks = fb_toast_create_chunk_hash();
	store->spill_file = -1;
	return store;
}

static void
fb_toast_close_spill_file(FbToastStore *store)
{
	if (store == NULL)
		return;

	if (store->spill_file >= 0)
	{
		FileClose(store->spill_file);
		store->spill_file = -1;
	}

	if (store->spill_path != NULL)
	{
		unlink(store->spill_path);
		pfree(store->spill_path);
		store->spill_path = NULL;
	}
}

void
fb_toast_store_destroy(FbToastStore *store)
{
	if (store == NULL)
		return;

	fb_toast_close_spill_file(store);
}

static void
fb_toast_ensure_spill_file(FbToastStore *store)
{
	char *runtime_dir;

	if (store == NULL || store->spill_file >= 0)
		return;

	fb_runtime_ensure_initialized();
	runtime_dir = fb_runtime_runtime_dir();
	store->spill_path = psprintf("%s/toast-retired-%d-%llu.bin",
								 runtime_dir,
								 MyProcPid,
								 (unsigned long long) ++fb_toast_spill_counter);
	pfree(runtime_dir);

	store->spill_file = PathNameOpenFile(store->spill_path,
										 O_CREAT | O_TRUNC | O_RDWR | PG_BINARY);
	if (store->spill_file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create fb toast spill file"),
				 errdetail("path=%s: %m", store->spill_path)));
}

static void
fb_toast_spill_chunk(FbToastStore *store,
					 FbToastChunkEntry *entry,
					 const char *data,
					 int32 len)
{
	if (store == NULL || entry == NULL || data == NULL || len <= 0)
		return;

	fb_toast_ensure_spill_file(store);
	if (FileWrite(store->spill_file, data, len, store->spill_size,
				  WAIT_EVENT_BUFFILE_WRITE) != len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write fb toast spill file"),
				 errdetail("path=%s: %m", store->spill_path)));

	entry->len = len;
	entry->data = NULL;
	entry->spilled = true;
	entry->spill_offset = store->spill_size;
	store->spill_size += len;
}

static char *
fb_toast_read_spilled_chunk(FbToastStore *store, const FbToastChunkEntry *entry)
{
	char *data;

	if (store == NULL || entry == NULL || !entry->spilled || entry->len <= 0)
		return NULL;

	if (store->spill_file < 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("fb toast spill file is not available for retired chunk")));

	data = palloc(entry->len);
	if (FileRead(store->spill_file, data, entry->len, entry->spill_offset,
				 WAIT_EVENT_BUFFILE_READ) != entry->len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read fb toast spill file"),
				 errdetail("path=%s: %m", store->spill_path)));

	return data;
}

static bool
fb_toast_extract_chunk(TupleDesc toast_tupdesc,
					   HeapTuple tuple,
					   Oid *chunk_id,
					   int32 *chunk_seq,
					   const char **chunk_data,
					   int32 *chunk_len)
{
	bool isnull;
	struct varlena *chunk;

	*chunk_id = DatumGetObjectId(fastgetattr(tuple, 1, toast_tupdesc, &isnull));
	if (isnull)
		return false;

	*chunk_seq = DatumGetInt32(fastgetattr(tuple, 2, toast_tupdesc, &isnull));
	if (isnull)
		return false;

	chunk = DatumGetByteaPP(fastgetattr(tuple, 3, toast_tupdesc, &isnull));
	if (isnull || chunk == NULL)
		return false;

	if (VARATT_IS_SHORT(chunk))
	{
		*chunk_len = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
		*chunk_data = VARDATA_SHORT(chunk);
	}
	else
	{
		*chunk_len = VARSIZE(chunk) - VARHDRSZ;
		*chunk_data = VARDATA(chunk);
	}

	return true;
}

static FbToastChunkEntry *
fb_toast_lookup_chunk_in_hash(HTAB *hash, Oid valueid, int32 chunk_seq)
{
	FbToastChunkKey key;

	if (hash == NULL)
		return NULL;

	MemSet(&key, 0, sizeof(key));
	key.valueid = valueid;
	key.chunk_seq = chunk_seq;

	return (FbToastChunkEntry *) hash_search(hash, &key, HASH_FIND, NULL);
}

static FbToastChunkEntry *
fb_toast_lookup_chunk(FbToastStore *store, Oid valueid, int32 chunk_seq)
{
	FbToastChunkEntry *entry;

	if (store == NULL)
		return NULL;

	entry = fb_toast_lookup_chunk_in_hash(store->live_chunks, valueid, chunk_seq);
	if (entry != NULL)
		return entry;

	return fb_toast_lookup_chunk_in_hash(store->retired_chunks, valueid, chunk_seq);
}

static const char *
fb_toast_get_chunk_bytes(FbToastStore *store,
						 const FbToastChunkEntry *entry,
						 bool *must_pfree)
{
	if (must_pfree != NULL)
		*must_pfree = false;

	if (entry == NULL)
		return NULL;

	if (entry->data != NULL)
		return entry->data;

	if (entry->spilled)
	{
		char *data = fb_toast_read_spilled_chunk(store, entry);

		if (must_pfree != NULL)
			*must_pfree = true;
		return data;
	}

	return NULL;
}

static struct varlena *
fb_toast_fetch_live_datum(const struct varatt_external *toast_pointer)
{
	Relation	toast_rel;
	Relation	toast_idx;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tuple;
	struct varlena *reconstructed;
	uint32		ext_size;
	uint32		data_done = 0;
	int32		expected_seq = 0;

	if (toast_pointer == NULL || !OidIsValid(toast_pointer->va_toastrelid))
		return NULL;

	toast_rel = table_open(toast_pointer->va_toastrelid, AccessShareLock);
	toast_idx = index_open(toast_get_valid_index(toast_pointer->va_toastrelid,
												 AccessShareLock),
						   AccessShareLock);

	ext_size = VARATT_EXTERNAL_GET_EXTSIZE(*toast_pointer);
	reconstructed = palloc0(toast_pointer->va_rawsize);

	ScanKeyInit(&key,
				(AttrNumber) 1,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(toast_pointer->va_valueid));
	scan = systable_beginscan_ordered(toast_rel, toast_idx, get_toast_snapshot(),
									  1, &key);

	while ((tuple = systable_getnext_ordered(scan, ForwardScanDirection)) != NULL)
	{
		Oid chunk_id;
		int32 chunk_seq;
		const char *chunk_data;
		int32 chunk_len;

		if (!fb_toast_extract_chunk(RelationGetDescr(toast_rel), tuple,
									&chunk_id, &chunk_seq,
									&chunk_data, &chunk_len))
			continue;

		if (chunk_id != toast_pointer->va_valueid)
			continue;

		if (chunk_seq != expected_seq)
		{
			data_done = 0;
			break;
		}

		if ((uint32) chunk_len > ext_size - data_done)
		{
			data_done = 0;
			break;
		}

		memcpy(VARDATA(reconstructed) + data_done, chunk_data, chunk_len);
		data_done += chunk_len;
		expected_seq++;

		if (data_done == ext_size)
			break;
	}

	systable_endscan_ordered(scan);
	index_close(toast_idx, AccessShareLock);
	table_close(toast_rel, AccessShareLock);

	if (data_done != ext_size)
		return NULL;

	if (VARATT_EXTERNAL_IS_COMPRESSED(*toast_pointer))
		SET_VARSIZE_COMPRESSED(reconstructed, data_done + VARHDRSZ);
	else
		SET_VARSIZE(reconstructed, data_done + VARHDRSZ);

	return reconstructed;
}

static char *
fb_toast_chunk_presence_summary(FbToastStore *store, Oid valueid)
{
	StringInfoData buf;
	int32 chunk_seq;
	int listed = 0;

	if (store == NULL)
		return pstrdup("store=null");

	initStringInfo(&buf);
	appendStringInfoString(&buf, "present=");

	for (chunk_seq = 0; chunk_seq < 16; chunk_seq++)
	{
		bool in_live = (fb_toast_lookup_chunk_in_hash(store->live_chunks, valueid, chunk_seq) != NULL);
		bool in_retired = (fb_toast_lookup_chunk_in_hash(store->retired_chunks, valueid, chunk_seq) != NULL);

		if (!in_live && !in_retired)
			continue;

		if (listed++ > 0)
			appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%d:%s%s",
						 chunk_seq,
						 in_live ? "L" : "",
						 in_retired ? "R" : "");
	}

	if (listed == 0)
		appendStringInfoString(&buf, "none");

	return buf.data;
}

void
fb_toast_store_put_tuple(FbToastStore *store,
						 TupleDesc toast_tupdesc,
						 HeapTuple tuple)
{
	Oid chunk_id;
	int32 chunk_seq;
	const char *chunk_data;
	int32 chunk_len;
	FbToastChunkKey key;
	FbToastChunkEntry *entry;
	bool found;

	if (store == NULL || toast_tupdesc == NULL || tuple == NULL)
		return;

	if (!fb_toast_extract_chunk(toast_tupdesc, tuple, &chunk_id, &chunk_seq,
								&chunk_data, &chunk_len))
		return;

	MemSet(&key, 0, sizeof(key));
	key.valueid = chunk_id;
	key.chunk_seq = chunk_seq;

	entry = (FbToastChunkEntry *) hash_search(store->live_chunks, &key, HASH_ENTER, &found);
	if (!found)
		MemSet(entry, 0, sizeof(*entry));
	else if (entry->data != NULL)
		pfree(entry->data);

	entry->key = key;
	entry->len = chunk_len;
	entry->data = palloc(chunk_len);
	entry->spilled = false;
	entry->spill_offset = 0;
	memcpy(entry->data, chunk_data, chunk_len);
}

void
fb_toast_store_remove_tuple(FbToastStore *store,
							TupleDesc toast_tupdesc,
							HeapTuple tuple)
{
	Oid chunk_id;
	int32 chunk_seq;
	const char *chunk_data;
	int32 chunk_len;
	FbToastChunkKey key;
	FbToastChunkEntry *entry;
	FbToastChunkEntry *retired;
	bool found;

	if (store == NULL || toast_tupdesc == NULL || tuple == NULL)
		return;

	if (!fb_toast_extract_chunk(toast_tupdesc, tuple, &chunk_id, &chunk_seq,
								&chunk_data, &chunk_len))
		return;

	MemSet(&key, 0, sizeof(key));
	key.valueid = chunk_id;
	key.chunk_seq = chunk_seq;

	entry = (FbToastChunkEntry *) hash_search(store->live_chunks, &key, HASH_FIND, NULL);
	retired = (FbToastChunkEntry *) hash_search(store->retired_chunks, &key, HASH_ENTER, &found);
	if (!found)
		MemSet(retired, 0, sizeof(*retired));
	else if (retired->data != NULL)
	{
		pfree(retired->data);
		retired->data = NULL;
	}

	retired->key = key;
	if (entry != NULL)
	{
		fb_toast_spill_chunk(store, retired, entry->data, entry->len);
		if (entry->data != NULL)
			pfree(entry->data);
		hash_search(store->live_chunks, &key, HASH_REMOVE, NULL);
	}
	else
	{
		fb_toast_spill_chunk(store, retired, chunk_data, chunk_len);
	}
}

void
fb_toast_store_sync_page(FbToastStore *store,
						 TupleDesc toast_tupdesc,
						 Page page,
						 BlockNumber blkno)
{
	OffsetNumber offnum;
	OffsetNumber maxoff;

	if (store == NULL || toast_tupdesc == NULL || page == NULL)
		return;

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
	{
		ItemId lp;
		HeapTupleData tuple;

		lp = PageGetItemId(page, offnum);
		if (!ItemIdIsNormal(lp))
			continue;

		MemSet(&tuple, 0, sizeof(tuple));
		ItemPointerSet(&tuple.t_self, blkno, offnum);
		tuple.t_tableOid = InvalidOid;
		tuple.t_len = ItemIdGetLength(lp);
		tuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);

		fb_toast_store_put_tuple(store, toast_tupdesc, &tuple);
	}
}

static struct varlena *
fb_toast_reconstruct_datum(FbToastStore *store,
						   struct varlena *attr)
{
	struct varatt_external toast_pointer;
	struct varlena *reconstructed;
	uint32 ext_size;
	uint32 data_done = 0;
	int32 chunk_seq = 0;

	if (attr == NULL || !VARATT_IS_EXTERNAL_ONDISK(attr))
		return NULL;

	VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);
	if (store == NULL)
		return fb_toast_fetch_live_datum(&toast_pointer);

	ext_size = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);
	reconstructed = palloc0(toast_pointer.va_rawsize);

		while (data_done < ext_size)
		{
			FbToastChunkEntry *entry;
			const char *chunk_bytes;
			bool free_chunk_bytes = false;

			entry = fb_toast_lookup_chunk(store, toast_pointer.va_valueid, chunk_seq);
			if (entry == NULL)
		{
			/*
			 * Some unchanged external datums remain live after target_ts and may
			 * only be partially represented in the historical toast store because
			 * replay only touched a subset of their TOAST blocks. If the original
			 * pointer still resolves against live TOAST, prefer that over raising
			 * a false historical-store miss.
			 */
			reconstructed = fb_toast_fetch_live_datum(&toast_pointer);
			if (reconstructed != NULL)
				return reconstructed;

			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("missing toast chunk %d for toast value %u in historical toast store",
							chunk_seq, toast_pointer.va_valueid),
					 errdetail("%s", fb_toast_chunk_presence_summary(store,
																 toast_pointer.va_valueid))));
		}

			if ((uint32) entry->len > ext_size - data_done)
				ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("toast chunk overflow for toast value %u", toast_pointer.va_valueid),
					 errdetail("chunk_seq=%d chunk_len=%d remaining=%u",
							   chunk_seq, entry->len, ext_size - data_done)));

			chunk_bytes = fb_toast_get_chunk_bytes(store, entry, &free_chunk_bytes);
			if (chunk_bytes == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("toast chunk payload is unavailable for toast value %u",
								toast_pointer.va_valueid),
						 errdetail("chunk_seq=%d", chunk_seq)));

			memcpy(VARDATA(reconstructed) + data_done, chunk_bytes, entry->len);
			if (free_chunk_bytes)
				pfree((void *) chunk_bytes);
			data_done += entry->len;
			chunk_seq++;
		}

	if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
		SET_VARSIZE_COMPRESSED(reconstructed, data_done + VARHDRSZ);
	else
		SET_VARSIZE(reconstructed, data_done + VARHDRSZ);

	return reconstructed;
}

HeapTuple
fb_toast_rewrite_tuple(FbToastStore *store,
					   TupleDesc tupdesc,
					   HeapTuple tuple)
{
	Datum *values;
	bool *nulls;
	HeapTuple rewritten;
	int natts = tupdesc->natts;
	int i;
	bool changed = false;

	if (tuple == NULL)
		return NULL;

	values = palloc0(sizeof(Datum) * natts);
	nulls = palloc0(sizeof(bool) * natts);
	heap_deform_tuple(tuple, tupdesc, values, nulls);

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		struct varlena *datum_ptr;
		struct varlena *reconstructed;

		if (attr->attisdropped || attr->attlen != -1 || nulls[i])
			continue;

		datum_ptr = (struct varlena *) DatumGetPointer(values[i]);
		if (datum_ptr == NULL || !VARATT_IS_EXTERNAL_ONDISK(datum_ptr))
			continue;

		reconstructed = fb_toast_reconstruct_datum(store, datum_ptr);
		if (reconstructed == NULL)
			continue;

		values[i] = PointerGetDatum(reconstructed);
		changed = true;
	}

	if (!changed)
		return heap_copytuple(tuple);

	rewritten = heap_form_tuple(tupdesc, values, nulls);
	rewritten->t_self = tuple->t_self;
	return rewritten;
}

bool
fb_toast_tuple_uses_external(TupleDesc tupdesc, HeapTuple tuple)
{
	Datum *values;
	bool *nulls;
	int natts = tupdesc->natts;
	int i;

	if (tuple == NULL)
		return false;

	values = palloc0(sizeof(Datum) * natts);
	nulls = palloc0(sizeof(bool) * natts);
	heap_deform_tuple(tuple, tupdesc, values, nulls);

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		struct varlena *datum_ptr;

		if (attr->attisdropped || attr->attlen != -1 || nulls[i])
			continue;

		datum_ptr = (struct varlena *) DatumGetPointer(values[i]);
		if (datum_ptr != NULL && VARATT_IS_EXTERNAL_ONDISK(datum_ptr))
			return true;
	}

	return false;
}
