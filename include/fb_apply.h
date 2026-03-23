#ifndef FB_APPLY_H
#define FB_APPLY_H

#include "postgres.h"

#include "access/htup_details.h"
#include "funcapi.h"
#include "utils/tuplestore.h"

#include "fb_common.h"
#include "fb_reverse_ops.h"

typedef void (*FbCurrentTupleVisitor) (HeapTuple tuple, TupleDesc tupdesc, void *arg);

void fb_apply_scan_current_relation(Oid relid,
									FbCurrentTupleVisitor visitor,
									void *arg);
void fb_apply_reverse_ops(const FbRelationInfo *info,
						  TupleDesc tupdesc,
						  const FbReverseOpStream *stream,
						  Tuplestorestate *tuplestore);
void fb_apply_keyed_mode(const FbRelationInfo *info,
						 TupleDesc tupdesc,
						 const FbReverseOpStream *stream,
						 Tuplestorestate *tuplestore);
void fb_apply_bag_mode(const FbRelationInfo *info,
					   TupleDesc tupdesc,
					   const FbReverseOpStream *stream,
					   Tuplestorestate *tuplestore);

#endif
