#ifndef FB_COMMON_H
#define FB_COMMON_H

#include "postgres.h"

#include "access/htup_details.h"
#include "access/itup.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "storage/relfilelocator.h"
#include "utils/timestamp.h"

typedef enum FbApplyMode
{
	FB_APPLY_KEYED = 1,
	FB_APPLY_BAG = 2
} FbApplyMode;

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
	AttrNumber key_attnums[INDEX_MAX_KEYS];
} FbRelationInfo;

#endif
