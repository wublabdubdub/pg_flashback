#ifndef FB_REVERSE_OPS_H
#define FB_REVERSE_OPS_H

#include "postgres.h"

#include "access/htup_details.h"

#include "fb_common.h"
#include "fb_wal.h"

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
	char *row_identity;
	char *key_identity;
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

typedef struct FbForwardOpStream
{
	FbForwardOp *ops;
	uint32 count;
	uint32 capacity;
	uint64 tracked_bytes;
	uint64 memory_limit_bytes;
} FbForwardOpStream;

typedef struct FbReverseOpStream
{
	FbReverseOp *ops;
	uint32 count;
	uint32 capacity;
	uint64 tracked_bytes;
	uint64 memory_limit_bytes;
} FbReverseOpStream;

void fb_build_forward_ops(const FbRelationInfo *info,
						  const FbWalRecordIndex *index,
						  TupleDesc tupdesc,
						  FbForwardOpStream *stream);
void fb_build_reverse_ops(const FbForwardOpStream *forward,
						  FbReverseOpStream *reverse);
char *fb_forward_ops_debug_summary(const FbForwardOpStream *stream);
char *fb_reverse_ops_debug_summary(const FbReverseOpStream *stream);

#endif
