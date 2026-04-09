/*
 * fb_catalog.h
 *    Relation catalog validation and apply-mode selection interfaces.
 */

#ifndef FB_CATALOG_H
#define FB_CATALOG_H

#include "fb_common.h"

/*
 * fb_catalog_load_relation_info
 *    Catalog API.
 */

void fb_catalog_load_relation_info(Oid relid, FbRelationInfo *info);
bool fb_catalog_relation_creation_precedes_target(Oid relid,
												  TimestampTz target_ts);

#endif
