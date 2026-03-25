#ifndef FB_APPLY_H
#define FB_APPLY_H

#include "postgres.h"

#include "access/htup_details.h"

#include "fb_common.h"
#include "fb_reverse_ops.h"

typedef void (*FbCurrentTupleVisitor) (HeapTuple tuple, TupleDesc tupdesc, void *arg);
typedef void (*FbResultTupleSinkFn) (HeapTuple tuple, TupleDesc tupdesc, void *arg);

typedef struct FbKeyedState FbKeyedState;
typedef struct FbBagState FbBagState;

typedef struct FbResultSink
{
	FbResultTupleSinkFn put_tuple;
	void *arg;
} FbResultSink;

void fb_apply_scan_current_relation(Oid relid,
									FbCurrentTupleVisitor visitor,
									void *arg,
									const char *progress_detail);
uint32 fb_apply_hash_identity(const char *identity);
char *fb_apply_build_key_identity(const FbRelationInfo *info,
								  HeapTuple tuple,
								  TupleDesc tupdesc);
char *fb_apply_build_row_identity(HeapTuple tuple,
								  TupleDesc tupdesc);

FbKeyedState *fb_keyed_state_create(const FbRelationInfo *info,
									TupleDesc tupdesc,
									uint64 tracked_bytes,
									uint64 memory_limit_bytes);
void fb_keyed_state_add_current_tuple(FbKeyedState *state,
									  HeapTuple tuple);
void fb_keyed_state_apply_add_tuple(FbKeyedState *state,
									HeapTuple tuple);
void fb_keyed_state_apply_remove_identity(FbKeyedState *state,
										  const char *identity);
void fb_keyed_state_apply_remove_tuple(FbKeyedState *state,
									   HeapTuple tuple);
uint64 fb_keyed_state_count_rows(FbKeyedState *state);
uint64 fb_keyed_state_emit_rows(FbKeyedState *state,
								TupleDesc tupdesc,
								const FbResultSink *sink);

FbBagState *fb_bag_state_create(TupleDesc tupdesc,
								uint64 tracked_bytes,
								uint64 memory_limit_bytes);
void fb_bag_state_add_current_tuple(FbBagState *state,
									HeapTuple tuple);
void fb_bag_state_apply_add_tuple(FbBagState *state,
								  HeapTuple tuple);
void fb_bag_state_apply_remove_tuple(FbBagState *state,
									 HeapTuple tuple);
uint64 fb_bag_state_count_rows(FbBagState *state);
uint64 fb_bag_state_emit_rows(FbBagState *state,
							  TupleDesc tupdesc,
							  const FbResultSink *sink);

uint64 fb_apply_reverse_ops(const FbRelationInfo *info,
							TupleDesc tupdesc,
							const FbReverseOpStream *stream,
							const FbResultSink *sink);
uint64 fb_apply_keyed_mode(const FbRelationInfo *info,
						   TupleDesc tupdesc,
						   const FbReverseOpStream *stream,
						   const FbResultSink *sink);
uint64 fb_apply_bag_mode(const FbRelationInfo *info,
						 TupleDesc tupdesc,
						 const FbReverseOpStream *stream,
						 const FbResultSink *sink);

#endif
