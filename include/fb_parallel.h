#ifndef FB_PARALLEL_H
#define FB_PARALLEL_H

#include "postgres.h"

#include "fb_common.h"
#include "fb_reverse_ops.h"

uint64 fb_parallel_apply_reverse_ops(const char *result_name,
									 const char *create_sql,
									 const FbRelationInfo *info,
									 TupleDesc tupdesc,
									 const FbReverseOpStream *stream);
bool fb_parallel_cleanup_result(const char *result_name);

#endif
