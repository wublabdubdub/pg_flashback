#ifndef FB_TOAST_H
#define FB_TOAST_H

#include "postgres.h"

#include "access/htup_details.h"
#include "storage/bufpage.h"
#include "utils/relcache.h"

#include "fb_common.h"

typedef struct FbToastStore FbToastStore;

FbToastStore *fb_toast_store_create(void);
void fb_toast_store_destroy(FbToastStore *store);
void fb_toast_store_put_tuple(FbToastStore *store,
							  TupleDesc toast_tupdesc,
							  HeapTuple tuple);
void fb_toast_store_remove_tuple(FbToastStore *store,
								 TupleDesc toast_tupdesc,
								 HeapTuple tuple);
void fb_toast_store_sync_page(FbToastStore *store,
							  TupleDesc toast_tupdesc,
							  Page page,
							  BlockNumber blkno);
HeapTuple fb_toast_rewrite_tuple(FbToastStore *store,
								 TupleDesc tupdesc,
								 HeapTuple tuple);
bool fb_toast_tuple_uses_external(TupleDesc tupdesc, HeapTuple tuple);

#endif
