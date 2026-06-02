/*
 * fb_apply.h
 *    Streaming reverse-op apply interfaces.
 */

#ifndef FB_APPLY_H
#define FB_APPLY_H

#include "postgres.h"

#include "access/htup_details.h"
#include "lib/stringinfo.h"

#include "fb_common.h"
#include "fb_reverse_ops.h"

typedef struct ExprContext ExprContext;
typedef struct FbApplyContext FbApplyContext;
typedef struct TupleTableSlot TupleTableSlot;
typedef struct FbKeyedResidualItem FbKeyedResidualItem;
typedef struct FbKeyedDeleteKey FbKeyedDeleteKey;

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
	bool owned_tuple;
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
							   const FbReverseOpSource *source,
							   const FbFastPathSpec *fast_path);
FbApplyContext *fb_apply_begin_count_only(const FbRelationInfo *info,
										  TupleDesc tupdesc,
										  uint64 row_count);
bool fb_apply_parallel_candidate(const FbRelationInfo *info,
								 const FbFastPathSpec *fast_path,
								 Oid relid);
/*
 * fb_apply_next
 *    Apply API.
 */

bool fb_apply_next(FbApplyContext *ctx, Datum *result);
bool fb_apply_next_slot(FbApplyContext *ctx, TupleTableSlot *slot);
TupleTableSlot *fb_apply_next_output_slot(FbApplyContext *ctx);
void fb_apply_bind_output_slot(FbApplyContext *ctx, TupleTableSlot *slot);
bool fb_apply_parallel_materialized(const FbApplyContext *ctx);
int fb_apply_parallel_log_count(const FbApplyContext *ctx);
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
void *fb_keyed_apply_begin_reader(const FbRelationInfo *info,
								  TupleDesc tupdesc,
								  FbReverseOpReader *reader,
								  long bucket_target,
								  uint64 tracked_bytes,
								  uint64 memory_limit_bytes,
								  int shard_id,
								  int shard_count);
void *fb_keyed_apply_begin_reader_ex(const FbRelationInfo *info,
									 TupleDesc tupdesc,
									 FbReverseOpReader *reader,
									 long bucket_target,
									 uint64 tracked_bytes,
									 uint64 memory_limit_bytes,
									 int shard_id,
									 int shard_count,
									 bool force_string_keys);
bool fb_keyed_apply_process_current(void *state,
									  TupleTableSlot *slot,
									  Datum *result);
bool fb_keyed_apply_process_current_emit(void *state,
										 TupleTableSlot *slot,
										 FbApplyEmit *emit);
bool fb_keyed_apply_process_current_emit_identity(void *state,
												  TupleTableSlot *slot,
												  FbApplyEmit *emit,
												  char **matched_identity_out);
void fb_keyed_apply_finish_scan(void *state);
uint64 fb_keyed_apply_residual_total(void *state);
bool fb_keyed_apply_next_residual(void *state, Datum *result);
bool fb_keyed_apply_next_residual_emit(void *state,
									   FbApplyEmit *emit);
bool fb_keyed_apply_emit_missing_key(void *state,
									 Datum key_value,
									 bool key_isnull,
									 FbApplyEmit *emit);
bool fb_keyed_apply_supports_single_typed_key(void *state);
bool fb_keyed_apply_supports_parallel_single_typed_key(void *state);
bool fb_keyed_apply_process_current_emit_typed_key(void *state,
												   TupleTableSlot *slot,
												   FbApplyEmit *emit,
												   Datum *matched_key_out,
												   bool *matched_isnull_out,
												   bool *matched_out);
void fb_keyed_apply_serialize_single_typed_key(void *state,
											   Datum key_value,
											   bool key_isnull,
											   StringInfo buf);
void fb_keyed_apply_mark_serialized_single_typed_seen(void *state,
													  const void *data,
													  Size len);
FbKeyedResidualItem *fb_keyed_apply_collect_residual_items(void *state,
														   uint64 *count_out);
FbKeyedDeleteKey *fb_keyed_apply_collect_delete_keys(void *state,
													 uint64 *count_out);
bool fb_keyed_apply_residual_item_ready(const FbKeyedResidualItem *item);
void fb_keyed_apply_residual_item_mark_emitted(FbKeyedResidualItem *item);
void fb_keyed_apply_mark_identity_seen(void *state, const char *identity);
void fb_keyed_apply_end(void *state);

struct FbKeyedResidualItem
{
	Datum key_value;
	bool key_isnull;
	HeapTuple tuple;
	void *cookie;
};

struct FbKeyedDeleteKey
{
	Datum key_value;
	bool key_isnull;
};

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
