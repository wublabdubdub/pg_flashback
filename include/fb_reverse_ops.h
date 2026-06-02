/*
 * fb_reverse_ops.h
 *    Reverse operation data structures, spill source, and readers.
 */

#ifndef FB_REVERSE_OPS_H
#define FB_REVERSE_OPS_H

#include "postgres.h"

#include "access/htup_details.h"

#include "fb_common.h"
#include "fb_spool.h"

typedef enum FbForwardOpType
{
	FB_FORWARD_INSERT = 1,
	FB_FORWARD_DELETE,
	FB_FORWARD_UPDATE
} FbForwardOpType;

typedef enum FbReverseOpType
{
	FB_REVERSE_REMOVE = 1,
	FB_REVERSE_ADD,
	FB_REVERSE_REPLACE
} FbReverseOpType;

typedef struct FbRowImage
{
	HeapTuple tuple;
	bool finalized;
} FbRowImage;

typedef struct FbForwardOp
{
	FbForwardOpType type;
	TransactionId xid;
	TimestampTz commit_ts;
	XLogRecPtr commit_lsn;
	XLogRecPtr record_lsn;
	FbRowImage old_row;
	FbRowImage new_row;
} FbForwardOp;

typedef struct FbReverseOp
{
	FbReverseOpType type;
	TransactionId xid;
	TimestampTz commit_ts;
	XLogRecPtr commit_lsn;
	XLogRecPtr record_lsn;
	FbRowImage old_row;
	FbRowImage new_row;
} FbReverseOp;

typedef struct FbReverseOpSource FbReverseOpSource;
typedef struct FbReverseOpReader FbReverseOpReader;

FbReverseOpSource *fb_reverse_source_create(FbSpoolSession *session,
											 uint64 *tracked_bytes,
											 uint64 memory_limit_bytes);
void fb_reverse_source_append(FbReverseOpSource *source, const FbReverseOp *op);
void fb_reverse_source_finish(FbReverseOpSource *source);
void fb_reverse_source_materialize(FbReverseOpSource *source);
void fb_reverse_source_destroy(FbReverseOpSource *source);
uint64 fb_reverse_source_tracked_bytes(const FbReverseOpSource *source);
uint64 fb_reverse_source_memory_limit_bytes(const FbReverseOpSource *source);
uint64 fb_reverse_source_total_count(const FbReverseOpSource *source);
Size fb_reverse_source_shared_size(const FbReverseOpSource *source);
void fb_reverse_source_write_shared(const FbReverseOpSource *source,
									void *dest,
									Size dest_size);
uint64 fb_reverse_source_shared_total_count(const void *shared_data);

FbReverseOpReader *fb_reverse_reader_open(const FbReverseOpSource *source);
FbReverseOpReader *fb_reverse_reader_open_shared(const void *shared_data);
bool fb_reverse_reader_next(FbReverseOpReader *reader, FbReverseOp *op);
void fb_reverse_reader_close(FbReverseOpReader *reader);

char *fb_reverse_ops_debug_summary(const FbReverseOpSource *source);

#endif
