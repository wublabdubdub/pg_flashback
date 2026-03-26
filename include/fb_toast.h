/*
 * fb_toast.h
 *    Historical TOAST reconstruction interfaces.
 */

#ifndef FB_TOAST_H
#define FB_TOAST_H

#include "postgres.h"

#include "access/htup_details.h"
#include "storage/bufpage.h"
#include "utils/relcache.h"

#include "fb_common.h"

/*
 * FbToastStore
 *    Opaque toast structure.
 */

typedef struct FbToastStore FbToastStore;

/*
 * fb_toast_store_create
 *    TOAST API.
 */

FbToastStore *fb_toast_store_create(void);
/*
 * fb_toast_store_destroy
 *    TOAST API.
 */

void fb_toast_store_destroy(FbToastStore *store);
/*
 * fb_toast_store_put_tuple
 *    TOAST API.
 */

void fb_toast_store_put_tuple(FbToastStore *store,
							  TupleDesc toast_tupdesc,
							  HeapTuple tuple);
/*
 * fb_toast_store_remove_tuple
 *    TOAST API.
 */

void fb_toast_store_remove_tuple(FbToastStore *store,
								 TupleDesc toast_tupdesc,
								 HeapTuple tuple);
/*
 * fb_toast_store_sync_page
 *    TOAST API.
 */

void fb_toast_store_sync_page(FbToastStore *store,
							  TupleDesc toast_tupdesc,
							  Page page,
							  BlockNumber blkno);
/*
 * fb_toast_rewrite_tuple
 *    TOAST API.
 */

HeapTuple fb_toast_rewrite_tuple(FbToastStore *store,
								 TupleDesc tupdesc,
								 HeapTuple tuple);
/*
 * fb_toast_tuple_uses_external
 *    TOAST API.
 */

bool fb_toast_tuple_uses_external(TupleDesc tupdesc, HeapTuple tuple);

#endif
