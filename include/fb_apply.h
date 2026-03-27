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
typedef struct TupleTableSlot TupleTableSlot;
typedef struct Tuplestorestate Tuplestorestate;

typedef enum FbApplyEmitKind
{
	FB_APPLY_EMIT_NONE = 0,
	FB_APPLY_EMIT_SLOT,
	FB_APPLY_EMIT_TUPLE
} FbApplyEmitKind;

typedef struct FbApplyEmit
{
	FbApplyEmitKind kind;
	TupleTableSlot *slot;
	HeapTuple tuple;
} FbApplyEmit;

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
char *fb_apply_build_key_identity_slot(const FbRelationInfo *info,
									   TupleTableSlot *slot,
									   TupleDesc tupdesc);
/*
 * fb_apply_build_row_identity
 *    Apply API.
 */

char *fb_apply_build_row_identity(HeapTuple tuple,
								  TupleDesc tupdesc);
char *fb_apply_build_row_identity_slot(TupleTableSlot *slot,
									   TupleDesc tupdesc);

/*
 * fb_apply_begin
 *    Apply API.
 */

FbApplyContext *fb_apply_begin(const FbRelationInfo *info,
								   TupleDesc tupdesc,
								   const FbReverseOpSource *source);
/*
 * fb_apply_next
 *    Apply API.
 */

bool fb_apply_next(FbApplyContext *ctx, Datum *result);
void fb_apply_materialize(FbApplyContext *ctx, Tuplestorestate *tupstore);
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
							   const FbReverseOpSource *source);
bool fb_keyed_apply_process_current(void *state,
									  TupleTableSlot *slot,
									  Datum *result);
bool fb_keyed_apply_process_current_emit(void *state,
										 TupleTableSlot *slot,
										 FbApplyEmit *emit);
void fb_keyed_apply_finish_scan(void *state);
uint64 fb_keyed_apply_residual_total(void *state);
bool fb_keyed_apply_next_residual(void *state, Datum *result);
bool fb_keyed_apply_next_residual_emit(void *state,
									   FbApplyEmit *emit);
void fb_keyed_apply_end(void *state);

/*
 * Internal bag mode hooks.
 */

void *fb_bag_apply_begin(const FbRelationInfo *info,
							 TupleDesc tupdesc,
							 const FbReverseOpSource *source);
bool fb_bag_apply_process_current(void *state,
								   TupleTableSlot *slot,
								   Datum *result);
bool fb_bag_apply_process_current_emit(void *state,
									   TupleTableSlot *slot,
									   FbApplyEmit *emit);
void fb_bag_apply_finish_scan(void *state);
uint64 fb_bag_apply_residual_total(void *state);
bool fb_bag_apply_next_residual(void *state, Datum *result);
bool fb_bag_apply_next_residual_emit(void *state,
									 FbApplyEmit *emit);
void fb_bag_apply_end(void *state);

#endif
