/*
 * fb_common.h
 *    Common relation metadata and shared flashback types.
 */

#ifndef FB_COMMON_H
#define FB_COMMON_H

#include "postgres.h"

#include "access/htup_details.h"
#include "access/itup.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "utils/timestamp.h"

#include "fb_compat.h"

typedef enum FbApplyMode
{
	FB_APPLY_KEYED = 1,
	FB_APPLY_BAG = 2
} FbApplyMode;

typedef enum FbFastPathMode
{
	FB_FAST_PATH_NONE = 0,
	FB_FAST_PATH_KEY_EQ,
	FB_FAST_PATH_KEY_IN,
	FB_FAST_PATH_KEY_RANGE,
	FB_FAST_PATH_KEY_TOPN
} FbFastPathMode;

typedef struct FbFastPathSpec
{
	FbFastPathMode mode;
	AttrNumber key_attnum;
	Oid key_type_oid;
	Oid key_collation;
	bool ordered_output;
	bool order_asc;
	uint64 limit_count;
	bool has_lower_bound;
	bool lower_inclusive;
	Datum lower_value;
	bool lower_isnull;
	bool has_upper_bound;
	bool upper_inclusive;
	Datum upper_value;
	bool upper_isnull;
	int key_count;
	Datum *key_values;
	bool *key_nulls;
} FbFastPathSpec;

/*
 * FbRelationInfo
 *    Describes relation metadata.
 */

typedef struct FbRelationInfo
{
	Oid relid;
	Oid toast_relid;
	RelFileLocator locator;
	RelFileLocator toast_locator;
	bool has_toast_locator;
	FbApplyMode mode;
	const char *mode_name;
	int key_natts;
	Oid key_index_oid;
	AttrNumber key_attnums[INDEX_MAX_KEYS];
} FbRelationInfo;

#endif
