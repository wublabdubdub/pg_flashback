/*
 * fb_catalog.c
 *    Relation catalog checks and apply-mode selection.
 */

#include "postgres.h"

#include "access/commit_ts.h"
#include "access/genam.h"
#include "access/relation.h"
#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "nodes/pg_list.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "fb_catalog.h"
#include "fb_error.h"

/*
 * fb_catalog_find_stable_unique_key
 *    Catalog helper.
 */

static bool
fb_catalog_find_stable_unique_key(Relation rel, FbRelationInfo *info)
{
	List *index_oids;
	ListCell *lc;
	bool found = false;

	index_oids = RelationGetIndexList(rel);

	foreach(lc, index_oids)
	{
		Oid index_oid = lfirst_oid(lc);
		Relation index_rel;
		Form_pg_index index_form;

		index_rel = index_open(index_oid, AccessShareLock);
		index_form = index_rel->rd_index;

		if ((index_form->indisprimary ||
			 (index_form->indisunique && index_form->indimmediate)) &&
			RelationGetIndexPredicate(index_rel) == NIL &&
			RelationGetIndexExpressions(index_rel) == NIL)
		{
			int i;

			found = true;
			info->key_index_oid = index_oid;
			info->key_natts = FB_INDEX_NKEYATTS(index_form);
			for (i = 0; i < info->key_natts; i++)
				info->key_attnums[i] = index_rel->rd_index->indkey.values[i];
			index_close(index_rel, AccessShareLock);
			break;
		}

		index_close(index_rel, AccessShareLock);
	}

	list_free(index_oids);

	return found;
}

/*
 * fb_catalog_choose_mode
 *    Catalog helper.
 */

static FbApplyMode
fb_catalog_choose_mode(Relation rel, FbRelationInfo *info)
{
	if (fb_catalog_find_stable_unique_key(rel, info))
		return FB_APPLY_KEYED;

	return FB_APPLY_BAG;
}

/*
 * fb_catalog_require_supported_relation
 *    Catalog helper.
 */

static void
fb_catalog_require_supported_relation(Relation rel)
{
	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		fb_raise_unsupported_relation("temporary relations");

	if (rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
		fb_raise_unsupported_relation("unlogged relations");

	if (rel->rd_rel->relkind == RELKIND_MATVIEW)
		fb_raise_unsupported_relation("materialized views");

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		fb_raise_unsupported_relation("partitioned table parents");

	if (IsCatalogRelation(rel))
		fb_raise_unsupported_relation("system catalogs");

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		fb_raise_unsupported_relation("this relation kind");
}

bool
fb_catalog_relation_creation_precedes_target(Oid relid, TimestampTz target_ts)
{
	HeapTuple tuple;
	TransactionId xmin;
	TimestampTz commit_ts = 0;
	RepOriginId origin_id = InvalidRepOriginId;
	bool found = false;

	if (!OidIsValid(relid) || target_ts == 0 || !track_commit_timestamp)
		return false;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return false;

	xmin = HeapTupleHeaderGetXmin(tuple->t_data);
	if (TransactionIdIsValid(xmin))
		found = TransactionIdGetCommitTsData(xmin, &commit_ts, &origin_id);

	ReleaseSysCache(tuple);

	return found && commit_ts != 0 && commit_ts <= target_ts;
}

/*
 * fb_catalog_load_relation_info
 *    Catalog entry point.
 */

void
fb_catalog_load_relation_info(Oid relid, FbRelationInfo *info)
{
	Relation rel;
	Relation toast_rel = NULL;
	FbApplyMode mode;

	if (info == NULL)
		elog(ERROR, "FbRelationInfo must not be NULL");

	MemSet(info, 0, sizeof(*info));

	rel = relation_open(relid, AccessShareLock);
	fb_catalog_require_supported_relation(rel);

	mode = fb_catalog_choose_mode(rel, info);

	info->relid = relid;
	info->toast_relid = rel->rd_rel->reltoastrelid;
	info->locator = FB_RELATION_LOCATOR(rel);
	info->mode = mode;
	info->mode_name = (mode == FB_APPLY_KEYED) ? "keyed" : "bag";

	if (OidIsValid(info->toast_relid))
	{
		toast_rel = relation_open(info->toast_relid, AccessShareLock);
		info->toast_locator = FB_RELATION_LOCATOR(toast_rel);
		info->has_toast_locator = true;
		relation_close(toast_rel, AccessShareLock);
	}

	relation_close(rel, AccessShareLock);
}
