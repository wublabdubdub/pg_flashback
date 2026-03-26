/*
 * fb_apply.h
 *    Streaming reverse-op apply interfaces.
 */

#ifndef FB_APPLY_H
#define FB_APPLY_H

#include "postgres.h"

#include "access/htup_details.h"

#include "fb_common.h"
#include "fb_reverse_ops.h"

typedef struct ExprContext ExprContext;
typedef struct FbApplyContext FbApplyContext;

/*
 * fb_apply_hash_identity
 *    Apply API.
 */

uint32 fb_apply_hash_identity(const char *identity);
/*
 * fb_apply_build_key_identity
 *    Apply API.
 */

char *fb_apply_build_key_identity(const FbRelationInfo *info,
								  HeapTuple tuple,
								  TupleDesc tupdesc);
/*
 * fb_apply_build_row_identity
 *    Apply API.
 */

char *fb_apply_build_row_identity(HeapTuple tuple,
								  TupleDesc tupdesc);

/*
 * fb_apply_begin
 *    Apply API.
 */

FbApplyContext *fb_apply_begin(const FbRelationInfo *info,
							   TupleDesc tupdesc,
							   const FbReverseOpStream *stream);
/*
 * fb_apply_next
 *    Apply API.
 */

HeapTuple fb_apply_next(FbApplyContext *ctx);
/*
 * fb_apply_end
 *    Apply API.
 */

void fb_apply_end(FbApplyContext *ctx);

/*
 * Internal keyed mode hooks.
 */

void *fb_keyed_apply_begin(const FbRelationInfo *info,
						   TupleDesc tupdesc,
						   const FbReverseOpStream *stream);
HeapTuple fb_keyed_apply_process_current(void *state, HeapTuple tuple);
void fb_keyed_apply_finish_scan(void *state);
uint64 fb_keyed_apply_residual_total(void *state);
HeapTuple fb_keyed_apply_next_residual(void *state);
void fb_keyed_apply_end(void *state);

/*
 * Internal bag mode hooks.
 */

void *fb_bag_apply_begin(const FbRelationInfo *info,
						 TupleDesc tupdesc,
						 const FbReverseOpStream *stream);
HeapTuple fb_bag_apply_process_current(void *state, HeapTuple tuple);
void fb_bag_apply_finish_scan(void *state);
uint64 fb_bag_apply_residual_total(void *state);
HeapTuple fb_bag_apply_next_residual(void *state);
void fb_bag_apply_end(void *state);

#endif
