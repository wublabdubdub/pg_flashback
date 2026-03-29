/*
 * fb_export.c
 *    Flashback mutation and export support.
 */

#include "postgres.h"

#include <limits.h>

#include "access/heapam.h"
#include "access/relation.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/dependency.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_constraint_d.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "parser/parse_relation.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "fb_apply.h"
#include "fb_catalog.h"
#include "fb_guc.h"
#include "fb_entry.h"
#include "fb_export.h"
#include "fb_progress.h"

#define FB_EXPORT_PARALLEL_MAX_WORKERS 8
#define FB_EXPORT_PARALLEL_TS_MAX 128
#define FB_EXPORT_PARALLEL_ERRMSG_MAX 512
#define FB_EXPORT_PARALLEL_MEMORY_LIMIT_MAX 64
#define FB_EXPORT_PARALLEL_SPILL_MODE_MAX 16
#define FB_EXPORT_PARALLEL_WORKERS_MAX 16
#define FB_EXPORT_PARALLEL_BOOL_MAX 8

typedef struct FbFlashbackExportTarget
{
	Oid namespace_oid;
	Oid relid;
	char *relname;
	char *qualified_name;
} FbFlashbackExportTarget;

typedef enum FbParallelExportRole
{
	FB_PARALLEL_EXPORT_ROLE_PREPARE = 1,
	FB_PARALLEL_EXPORT_ROLE_SHARD,
	FB_PARALLEL_EXPORT_ROLE_FINALIZE,
	FB_PARALLEL_EXPORT_ROLE_CLEANUP
} FbParallelExportRole;

typedef enum FbParallelExportStatus
{
	FB_PARALLEL_EXPORT_STATUS_INIT = 0,
	FB_PARALLEL_EXPORT_STATUS_RUNNING,
	FB_PARALLEL_EXPORT_STATUS_DONE,
	FB_PARALLEL_EXPORT_STATUS_ERROR
} FbParallelExportStatus;

typedef struct FbParallelExportTask
{
	int role;
	int status;
	Oid dboid;
	Oid useroid;
	Oid source_relid;
	Oid namespace_oid;
	Oid created_relid;
	int shard_id;
	int shard_count;
	uint64 rowcount;
	char target_ts[FB_EXPORT_PARALLEL_TS_MAX];
	char final_relname[NAMEDATALEN];
	char stage_relname[NAMEDATALEN];
	char archive_dest[MAXPGPATH];
	char archive_dir[MAXPGPATH];
	char debug_pg_wal_dir[MAXPGPATH];
	char memory_limit[FB_EXPORT_PARALLEL_MEMORY_LIMIT_MAX];
	char spill_mode[FB_EXPORT_PARALLEL_SPILL_MODE_MAX];
	char parallel_workers[FB_EXPORT_PARALLEL_WORKERS_MAX];
	char show_progress[FB_EXPORT_PARALLEL_BOOL_MAX];
	char errmsg[FB_EXPORT_PARALLEL_ERRMSG_MAX];
} FbParallelExportTask;

typedef struct FbParallelExportShared
{
	int task_count;
	int worker_count;
	Size tasks_offset;
	Size reverse_shared_offset;
	Size reverse_shared_size;
	Size scan_shared_offset;
	Size scan_shared_size;
	char data[FLEXIBLE_ARRAY_MEMBER];
} FbParallelExportShared;

PG_FUNCTION_INFO_V1(pg_flashback_to);
PG_FUNCTION_INFO_V1(pg_flashback_rewind);

PGDLLEXPORT void fb_parallel_export_worker_main(Datum main_arg);

static uint64 fb_export_load_rows(Oid source_relid,
								  Oid target_relid,
								  text *target_ts_text);
static void fb_parallel_export_fill_task(FbParallelExportTask *task,
										 FbParallelExportRole role,
										 Oid source_relid,
										 Oid namespace_oid,
										 const char *final_relname,
										 const char *stage_relname,
										 const char *target_ts,
										 int shard_id,
										 int shard_count);
static void fb_parallel_export_capture_gucs(FbParallelExportTask *task);
static void fb_parallel_export_apply_gucs(const FbParallelExportTask *task);
static bool fb_export_parallel_supported(const FbRelationInfo *info);
static Oid fb_export_parallel_run(Oid source_relid,
								  text *target_ts_text,
								  const FbRelationInfo *info);
static void fb_rewind_require_supported(Oid source_relid,
										const FbRelationInfo *info,
										TupleDesc tupdesc);
static bool fb_rewind_spi_relation_flag(const char *sql, Oid relid);
static void fb_rewind_check_constraints_and_triggers(Oid source_relid);
static char *fb_rewind_temp_relname(const char *prefix);
static Oid fb_rewind_create_upsert_table(Oid source_relid, const char *relname);
static Oid fb_rewind_create_delete_table(const char *relname,
										 const char *key_colname,
										 Oid key_type_oid,
										 int32 key_typmod);
static void fb_rewind_load_upsert_rows(Oid relid,
									   const FbKeyedResidualItem *items,
									   uint64 count);
static void fb_rewind_load_delete_keys(Oid relid,
									   const FbKeyedDeleteKey *items,
									   uint64 count);
static bool fb_rewind_attr_insertable(Form_pg_attribute attr);
static bool fb_rewind_attr_updatable(Form_pg_attribute attr,
									 AttrNumber key_attnum);
static char *fb_rewind_build_insert_column_list(TupleDesc tupdesc,
												bool *needs_identity_override);
static char *fb_rewind_build_update_set_list(TupleDesc tupdesc,
											 AttrNumber key_attnum);
static uint64 fb_rewind_apply_source_dml(Oid source_relid,
										 const char *upsert_relname,
										 const char *delete_relname,
										 TupleDesc tupdesc,
										 AttrNumber key_attnum);
static FbParallelExportTask *fb_parallel_export_tasks(FbParallelExportShared *shared);
static void *fb_parallel_export_reverse_shared(FbParallelExportShared *shared);
static const void *fb_parallel_export_reverse_shared_const(const FbParallelExportShared *shared);
static uint64 fb_parallel_export_load_stage_shared(const FbParallelExportTask *task,
												   FbParallelExportShared *shared,
												   const FbRelationInfo *info);

static FbParallelExportTask *
fb_parallel_export_tasks(FbParallelExportShared *shared)
{
	return (FbParallelExportTask *) ((char *) shared + shared->tasks_offset);
}

static void *
fb_parallel_export_reverse_shared(FbParallelExportShared *shared)
{
	return (char *) shared + shared->reverse_shared_offset;
}

static const void *
fb_parallel_export_reverse_shared_const(const FbParallelExportShared *shared)
{
	return (const char *) shared + shared->reverse_shared_offset;
}

static char *
fb_export_target_relname(Oid source_relid, Oid *namespace_oid_out)
{
	Relation source_rel;
	char *target_relname;

	source_rel = relation_open(source_relid, AccessShareLock);
	*namespace_oid_out = RelationGetNamespace(source_rel);
	target_relname = makeObjectName(RelationGetRelationName(source_rel),
									"flashback",
									NULL);
	relation_close(source_rel, AccessShareLock);

	return target_relname;
}

static char *
fb_export_qualified_relation_name(Oid namespace_oid, const char *relname)
{
	char *namespace_name = get_namespace_name(namespace_oid);

	if (namespace_name == NULL || relname == NULL)
		elog(ERROR, "could not resolve relation name for export");

	return quote_qualified_identifier(namespace_name, relname);
}

static void
fb_export_exec_ddl(const char *sql)
{
	int spi_rc;

	spi_rc = SPI_execute(sql, false, 0);
	if (spi_rc < 0)
		elog(ERROR, "failed to execute DDL: %s", sql);

	CommandCounterIncrement();
}

static void
fb_export_ensure_target_absent(Oid namespace_oid, const char *relname)
{
	if (OidIsValid(get_relname_relid(relname, namespace_oid)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists", relname)));
}

static void
fb_parallel_export_copy_setting(char *dst, Size dstlen, const char *name)
{
	char *value;

	value = GetConfigOptionByName(name, NULL, false);
	if (value == NULL)
		elog(ERROR, "could not read GUC \"%s\" for parallel export", name);
	if (strlen(value) >= dstlen)
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("GUC \"%s\" is too long for parallel export worker state", name)));

	strlcpy(dst, value, dstlen);
}

static void
fb_parallel_export_capture_gucs(FbParallelExportTask *task)
{
	fb_parallel_export_copy_setting(task->archive_dest,
									sizeof(task->archive_dest),
									"pg_flashback.archive_dest");
	fb_parallel_export_copy_setting(task->archive_dir,
									sizeof(task->archive_dir),
									"pg_flashback.archive_dir");
	fb_parallel_export_copy_setting(task->debug_pg_wal_dir,
									sizeof(task->debug_pg_wal_dir),
									"pg_flashback.debug_pg_wal_dir");
	fb_parallel_export_copy_setting(task->memory_limit,
									sizeof(task->memory_limit),
									"pg_flashback.memory_limit");
	fb_parallel_export_copy_setting(task->spill_mode,
									sizeof(task->spill_mode),
									"pg_flashback.spill_mode");
	fb_parallel_export_copy_setting(task->parallel_workers,
									sizeof(task->parallel_workers),
									"pg_flashback.parallel_workers");
	fb_parallel_export_copy_setting(task->show_progress,
									sizeof(task->show_progress),
									"pg_flashback.show_progress");
}

static void
fb_parallel_export_apply_gucs(const FbParallelExportTask *task)
{
	SetConfigOption("pg_flashback.archive_dest",
					task->archive_dest,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.archive_dir",
					task->archive_dir,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.debug_pg_wal_dir",
					task->debug_pg_wal_dir,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.memory_limit",
					task->memory_limit,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.spill_mode",
					task->spill_mode,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.parallel_workers",
					task->parallel_workers,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.show_progress",
					task->show_progress,
					PGC_USERSET,
					PGC_S_SESSION);
}

static void
fb_export_create_base_table(Oid source_relid, FbFlashbackExportTarget *target)
{
	char *source_qname;
	StringInfoData ddl;

	source_qname = fb_export_qualified_relation_name(get_rel_namespace(source_relid),
													 get_rel_name(source_relid));
	target->qualified_name = fb_export_qualified_relation_name(target->namespace_oid,
																target->relname);
	fb_export_ensure_target_absent(target->namespace_oid, target->relname);

	initStringInfo(&ddl);
	appendStringInfo(&ddl, "CREATE TABLE %s (LIKE %s",
					 target->qualified_name,
					 source_qname);
	appendStringInfoString(&ddl, " INCLUDING DEFAULTS");
	appendStringInfoString(&ddl, " INCLUDING CONSTRAINTS");
	appendStringInfoString(&ddl, " INCLUDING STORAGE");
#if PG_VERSION_NUM >= 100000
	appendStringInfoString(&ddl, " INCLUDING IDENTITY");
#endif
#if PG_VERSION_NUM >= 120000
	appendStringInfoString(&ddl, " INCLUDING GENERATED");
#endif
#if PG_VERSION_NUM >= 140000
	appendStringInfoString(&ddl, " INCLUDING COMPRESSION");
#endif
	appendStringInfoChar(&ddl, ')');

	fb_export_exec_ddl(ddl.data);

	target->relid = get_relname_relid(target->relname, target->namespace_oid);
	if (!OidIsValid(target->relid))
		elog(ERROR, "could not resolve flashback export target relation");
}

static uint64
fb_export_load_rows(Oid source_relid,
					Oid target_relid,
					text *target_ts_text)
{
	FbFlashbackQueryState *query = NULL;
	Relation target_rel = NULL;
	TupleTableSlot *slot = NULL;
	BulkInsertState bistate = NULL;
	CommandId cid;
	uint64 rowcount = 0;

	query = fb_flashback_query_begin(source_relid, target_ts_text, NULL);
	target_rel = relation_open(target_relid, RowExclusiveLock);
	slot = table_slot_create(target_rel, NULL);
	bistate = GetBulkInsertState();
	cid = GetCurrentCommandId(true);

	PG_TRY();
	{
		while (fb_flashback_query_next_slot(query, slot))
		{
			table_tuple_insert(target_rel,
							   slot,
							   cid,
							   TABLE_INSERT_SKIP_FSM,
							   bistate);
			ExecClearTuple(slot);
			rowcount++;
		}

		table_finish_bulk_insert(target_rel, TABLE_INSERT_SKIP_FSM);
		FreeBulkInsertState(bistate);
		bistate = NULL;
		ExecDropSingleTupleTableSlot(slot);
		slot = NULL;
		relation_close(target_rel, RowExclusiveLock);
		target_rel = NULL;
		fb_flashback_query_finish(query);
		query = NULL;
	}
	PG_CATCH();
	{
		if (bistate != NULL)
			FreeBulkInsertState(bistate);
		if (slot != NULL)
			ExecDropSingleTupleTableSlot(slot);
		if (target_rel != NULL)
			relation_close(target_rel, RowExclusiveLock);
		if (query != NULL)
			fb_flashback_query_abort(query);
		PG_RE_THROW();
	}
	PG_END_TRY();

	CommandCounterIncrement();
	return rowcount;
}

static char *
fb_export_choose_constraint_name(Oid namespace_oid,
								 const char *target_relname,
								 char contype)
{
	if (contype == CONSTRAINT_PRIMARY)
		return ChooseConstraintName(target_relname, NULL, "pkey", namespace_oid, NIL);
	if (contype == CONSTRAINT_UNIQUE)
		return ChooseConstraintName(target_relname, NULL, "key", namespace_oid, NIL);

	elog(ERROR, "unsupported constraint type for flashback export: %c", contype);
	return NULL;
}

static void
fb_export_add_pk_unique_constraints(Oid source_relid,
									const FbFlashbackExportTarget *target)
{
	Relation source_rel;
	List *index_oids;
	ListCell *lc;

	source_rel = relation_open(source_relid, AccessShareLock);
	index_oids = RelationGetIndexList(source_rel);

	foreach(lc, index_oids)
	{
		Oid index_oid = lfirst_oid(lc);
		Oid constraint_oid = get_index_constraint(index_oid);
		HeapTuple constraint_tuple;
		Form_pg_constraint constraint_form;
		char *constraint_name;
		char *constraint_def;
		char *sql;

		if (!OidIsValid(constraint_oid))
			continue;

		constraint_tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraint_oid));
		if (!HeapTupleIsValid(constraint_tuple))
			elog(ERROR, "cache lookup failed for constraint %u", constraint_oid);

		constraint_form = (Form_pg_constraint) GETSTRUCT(constraint_tuple);
		if (constraint_form->contype != CONSTRAINT_PRIMARY &&
			constraint_form->contype != CONSTRAINT_UNIQUE)
		{
			ReleaseSysCache(constraint_tuple);
			continue;
		}

		constraint_name = fb_export_choose_constraint_name(target->namespace_oid,
														   target->relname,
														   constraint_form->contype);
		constraint_def =
			TextDatumGetCString(OidFunctionCall1(F_PG_GET_CONSTRAINTDEF_OID,
												 ObjectIdGetDatum(constraint_oid)));
		sql = psprintf("ALTER TABLE %s ADD CONSTRAINT %s %s",
					   target->qualified_name,
					   quote_identifier(constraint_name),
					   constraint_def);
		fb_export_exec_ddl(sql);
		ReleaseSysCache(constraint_tuple);
	}

	list_free(index_oids);
	relation_close(source_rel, AccessShareLock);
}

static char *
fb_export_build_index_ddl(const char *indexdef,
						  const char *new_index_name,
						  const char *target_qualified_name)
{
	const char *using_pos;
	const char *prefix;

	using_pos = strstr(indexdef, " USING ");
	if (using_pos == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_flashback_to could not rewrite source index definition"),
				 errdetail_internal("indexdef=%s", indexdef)));

	if (strncmp(indexdef, "CREATE UNIQUE INDEX ", strlen("CREATE UNIQUE INDEX ")) == 0)
		prefix = "CREATE UNIQUE INDEX";
	else if (strncmp(indexdef, "CREATE INDEX ", strlen("CREATE INDEX ")) == 0)
		prefix = "CREATE INDEX";
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_flashback_to only supports plain CREATE INDEX definitions"),
				 errdetail_internal("indexdef=%s", indexdef)));

	return psprintf("%s %s ON %s%s",
					prefix,
					quote_identifier(new_index_name),
					target_qualified_name,
					using_pos);
}

static void
fb_export_add_secondary_indexes(Oid source_relid,
								const FbFlashbackExportTarget *target)
{
	Relation source_rel;
	List *index_oids;
	ListCell *lc;

	source_rel = relation_open(source_relid, AccessShareLock);
	index_oids = RelationGetIndexList(source_rel);

	foreach(lc, index_oids)
	{
		Oid index_oid = lfirst_oid(lc);
		Oid constraint_oid = get_index_constraint(index_oid);
		Relation index_rel;
		const char *index_name;
		char *new_index_name;
		char *indexdef;
		char *sql;

		if (OidIsValid(constraint_oid))
			continue;

		index_rel = index_open(index_oid, AccessShareLock);
		index_name = RelationGetRelationName(index_rel);
		if (!index_rel->rd_index->indisready || !index_rel->rd_index->indisvalid)
		{
			index_close(index_rel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("pg_flashback_to does not support invalid source indexes"),
					 errdetail_internal("index=%s", index_name)));
		}

		new_index_name = ChooseRelationName(target->relname,
											 NULL,
											 "idx",
											 target->namespace_oid,
											 false);
		indexdef = pg_get_indexdef_string(index_oid);
		sql = fb_export_build_index_ddl(indexdef,
										 new_index_name,
										 target->qualified_name);
		index_close(index_rel, AccessShareLock);
		fb_export_exec_ddl(sql);
	}

	list_free(index_oids);
	relation_close(source_rel, AccessShareLock);
}

static bool
fb_export_parallel_supported(const FbRelationInfo *info)
{
	if (fb_export_parallel_workers() <= 1)
		return false;
	if (info == NULL || info->mode != FB_APPLY_KEYED || info->key_natts != 1)
		return false;
	if (IsTransactionBlock())
		return false;

	return true;
}

static void
fb_rewind_require_supported(Oid source_relid,
							 const FbRelationInfo *info,
							 TupleDesc tupdesc)
{
	if (info == NULL || tupdesc == NULL)
		elog(ERROR, "invalid pg_flashback_to relation state");
	if (info->mode != FB_APPLY_KEYED || info->key_natts != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_flashback_to currently requires a keyed relation with a single stable key")));

	fb_rewind_check_constraints_and_triggers(source_relid);
}

static bool
fb_rewind_spi_relation_flag(const char *sql, Oid relid)
{
	Oid argtypes[1];
	Datum values[1];
	const char nulls[1] = {' '};
	int spi_rc;
	bool isnull = false;

	argtypes[0] = REGCLASSOID;
	values[0] = ObjectIdGetDatum(relid);
	spi_rc = SPI_execute_with_args(sql, 1, argtypes, values, nulls, true, 1);
	if (spi_rc != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute_with_args failed for pg_flashback_to guard");
	if (SPI_processed != 1 || SPI_tuptable == NULL)
		elog(ERROR, "pg_flashback_to guard query returned no row");

	return DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc,
									  1,
									  &isnull));
}

static void
fb_rewind_check_constraints_and_triggers(Oid source_relid)
{
	static const char *fk_sql =
		"SELECT EXISTS ("
		"SELECT 1 FROM pg_constraint "
		"WHERE contype = 'f' AND (conrelid = $1 OR confrelid = $1))";
	static const char *trigger_sql =
		"SELECT EXISTS ("
		"SELECT 1 FROM pg_trigger "
		"WHERE tgrelid = $1 AND NOT tgisinternal)";
	int spi_rc;
	bool has_fk;
	bool has_user_trigger;

	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %d", spi_rc);

	has_fk = fb_rewind_spi_relation_flag(fk_sql, source_relid);
	has_user_trigger = fb_rewind_spi_relation_flag(trigger_sql, source_relid);
	SPI_finish();

	if (has_fk)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_flashback_to does not support relations with foreign keys")));
	if (has_user_trigger)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_flashback_to does not support relations with user triggers")));
}

static char *
fb_rewind_temp_relname(const char *prefix)
{
	return psprintf("%s_%d", prefix, MyProcPid);
}

static Oid
fb_rewind_create_upsert_table(Oid source_relid, const char *relname)
{
	char *source_qname;
	StringInfoData ddl;
	Oid relid;

	source_qname = fb_export_qualified_relation_name(get_rel_namespace(source_relid),
													 get_rel_name(source_relid));
	initStringInfo(&ddl);
	appendStringInfo(&ddl, "CREATE TEMP TABLE %s (LIKE %s",
					 quote_identifier(relname),
					 source_qname);
	appendStringInfoString(&ddl, " INCLUDING DEFAULTS");
	appendStringInfoString(&ddl, " INCLUDING STORAGE");
#if PG_VERSION_NUM >= 100000
	appendStringInfoString(&ddl, " INCLUDING IDENTITY");
#endif
#if PG_VERSION_NUM >= 120000
	appendStringInfoString(&ddl, " INCLUDING GENERATED");
#endif
#if PG_VERSION_NUM >= 140000
	appendStringInfoString(&ddl, " INCLUDING COMPRESSION");
#endif
	appendStringInfoString(&ddl, ") ON COMMIT DROP");
	fb_export_exec_ddl(ddl.data);

	relid = RelnameGetRelid(relname);
	if (!OidIsValid(relid))
		elog(ERROR, "could not resolve pg_flashback_to upsert temp relation");
	return relid;
}

static Oid
fb_rewind_create_delete_table(const char *relname,
							  const char *key_colname,
							  Oid key_type_oid,
							  int32 key_typmod)
{
	char *type_sql;
	char *sql;
	Oid relid;

	type_sql = format_type_with_typemod(key_type_oid, key_typmod);
	sql = psprintf("CREATE TEMP TABLE %s (%s %s) ON COMMIT DROP",
				   quote_identifier(relname),
				   quote_identifier(key_colname),
				   type_sql);
	fb_export_exec_ddl(sql);

	relid = RelnameGetRelid(relname);
	if (!OidIsValid(relid))
		elog(ERROR, "could not resolve pg_flashback_to delete temp relation");
	return relid;
}

static void
fb_rewind_load_upsert_rows(Oid relid,
						   const FbKeyedResidualItem *items,
						   uint64 count)
{
	Relation rel;
	TupleTableSlot *slot;
	BulkInsertState bistate;
	CommandId cid;
	uint64 i;

	if (count == 0 || items == NULL)
		return;

	rel = relation_open(relid, RowExclusiveLock);
	slot = table_slot_create(rel, NULL);
	bistate = GetBulkInsertState();
	cid = GetCurrentCommandId(true);

	PG_TRY();
	{
		for (i = 0; i < count; i++)
		{
			if (items[i].tuple == NULL)
				continue;
			ExecForceStoreHeapTuple(items[i].tuple, slot, false);
			table_tuple_insert(rel,
							   slot,
							   cid,
							   TABLE_INSERT_SKIP_FSM,
							   bistate);
			ExecClearTuple(slot);
		}

		table_finish_bulk_insert(rel, TABLE_INSERT_SKIP_FSM);
		FreeBulkInsertState(bistate);
		ExecDropSingleTupleTableSlot(slot);
		relation_close(rel, RowExclusiveLock);
	}
	PG_CATCH();
	{
		FreeBulkInsertState(bistate);
		ExecDropSingleTupleTableSlot(slot);
		relation_close(rel, RowExclusiveLock);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
fb_rewind_load_delete_keys(Oid relid,
						   const FbKeyedDeleteKey *items,
						   uint64 count)
{
	Relation rel;
	TupleTableSlot *slot;
	BulkInsertState bistate;
	CommandId cid;
	uint64 i;

	if (count == 0 || items == NULL)
		return;

	rel = relation_open(relid, RowExclusiveLock);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &TTSOpsVirtual);
	bistate = GetBulkInsertState();
	cid = GetCurrentCommandId(true);

	PG_TRY();
	{
		for (i = 0; i < count; i++)
		{
			ExecClearTuple(slot);
			slot->tts_values[0] = items[i].key_value;
			slot->tts_isnull[0] = items[i].key_isnull;
			ExecStoreVirtualTuple(slot);
			table_tuple_insert(rel,
							   slot,
							   cid,
							   TABLE_INSERT_SKIP_FSM,
							   bistate);
			ExecClearTuple(slot);
		}

		table_finish_bulk_insert(rel, TABLE_INSERT_SKIP_FSM);
		FreeBulkInsertState(bistate);
		ExecDropSingleTupleTableSlot(slot);
		relation_close(rel, RowExclusiveLock);
	}
	PG_CATCH();
	{
		FreeBulkInsertState(bistate);
		ExecDropSingleTupleTableSlot(slot);
		relation_close(rel, RowExclusiveLock);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static bool
fb_rewind_attr_insertable(Form_pg_attribute attr)
{
	if (attr->attisdropped)
		return false;
	if (attr->attgenerated != '\0')
		return false;

	return true;
}

static bool
fb_rewind_attr_updatable(Form_pg_attribute attr,
						 AttrNumber key_attnum)
{
	if (!fb_rewind_attr_insertable(attr))
		return false;
	if (attr->attnum == key_attnum)
		return false;
	if (attr->attidentity != '\0')
		return false;

	return true;
}

static char *
fb_rewind_build_insert_column_list(TupleDesc tupdesc,
								   bool *needs_identity_override)
{
	StringInfoData buf;
	int i;

	if (needs_identity_override != NULL)
		*needs_identity_override = false;
	initStringInfo(&buf);
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (!fb_rewind_attr_insertable(attr))
			continue;
		if (buf.len > 0)
			appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, quote_identifier(NameStr(attr->attname)));
		if (needs_identity_override != NULL && attr->attidentity != '\0')
			*needs_identity_override = true;
	}

	return (buf.len == 0) ? NULL : buf.data;
}

static char *
fb_rewind_build_update_set_list(TupleDesc tupdesc,
								AttrNumber key_attnum)
{
	StringInfoData buf;
	int i;

	initStringInfo(&buf);
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		const char *attname;

		if (!fb_rewind_attr_updatable(attr, key_attnum))
			continue;
		attname = NameStr(attr->attname);
		if (buf.len > 0)
			appendStringInfoString(&buf, ", ");
		appendStringInfo(&buf, "%s = src.%s",
						 quote_identifier(attname),
						 quote_identifier(attname));
	}

	return (buf.len == 0) ? NULL : buf.data;
}

static uint64
fb_rewind_apply_source_dml(Oid source_relid,
						   const char *upsert_relname,
						   const char *delete_relname,
						   TupleDesc tupdesc,
						   AttrNumber key_attnum)
{
	Form_pg_attribute key_attr;
	char *source_qname;
	char *upsert_qname;
	char *delete_qname;
	char *key_name;
	char *insert_cols;
	char *update_set;
	bool needs_identity_override;
	StringInfoData sql;
	int spi_rc;
	uint64 affected = 0;

	key_attr = TupleDescAttr(tupdesc, key_attnum - 1);
	source_qname = fb_export_qualified_relation_name(get_rel_namespace(source_relid),
													 get_rel_name(source_relid));
	upsert_qname = quote_qualified_identifier("pg_temp", upsert_relname);
	delete_qname = quote_qualified_identifier("pg_temp", delete_relname);
	key_name = quote_identifier(NameStr(key_attr->attname));
	insert_cols = fb_rewind_build_insert_column_list(tupdesc, &needs_identity_override);
	update_set = fb_rewind_build_update_set_list(tupdesc, key_attnum);

	if (update_set != NULL)
	{
		initStringInfo(&sql);
		appendStringInfo(&sql,
						 "UPDATE %s AS dst "
						 "SET %s "
						 "FROM %s AS src "
						 "WHERE dst.%s IS NOT DISTINCT FROM src.%s",
						 source_qname,
						 update_set,
						 upsert_qname,
						 key_name,
						 key_name);
		spi_rc = SPI_execute(sql.data, false, 0);
		if (spi_rc < 0)
			elog(ERROR, "failed to execute pg_flashback_to update");
		affected += SPI_processed;
		CommandCounterIncrement();
	}

	if (insert_cols != NULL)
	{
		initStringInfo(&sql);
		appendStringInfo(&sql,
						 "INSERT INTO %s (%s) %s "
						 "SELECT %s FROM %s AS src "
						 "WHERE NOT EXISTS ("
						 "SELECT 1 FROM %s AS dst "
						 "WHERE dst.%s IS NOT DISTINCT FROM src.%s)",
						 source_qname,
						 insert_cols,
						 needs_identity_override ? "OVERRIDING SYSTEM VALUE" : "",
						 insert_cols,
						 upsert_qname,
						 source_qname,
						 key_name,
						 key_name);
		spi_rc = SPI_execute(sql.data, false, 0);
		if (spi_rc < 0)
			elog(ERROR, "failed to execute pg_flashback_to insert");
		affected += SPI_processed;
		CommandCounterIncrement();
	}

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "DELETE FROM %s AS dst "
					 "USING %s AS del "
					 "WHERE dst.%s IS NOT DISTINCT FROM del.%s",
					 source_qname,
					 delete_qname,
					 key_name,
					 key_name);
	spi_rc = SPI_execute(sql.data, false, 0);
	if (spi_rc < 0)
		elog(ERROR, "failed to execute pg_flashback_to delete");
	affected += SPI_processed;
	CommandCounterIncrement();

	return affected;
}

static void
fb_parallel_export_fill_task(FbParallelExportTask *task,
							 FbParallelExportRole role,
							 Oid source_relid,
							 Oid namespace_oid,
							 const char *final_relname,
							 const char *stage_relname,
							 const char *target_ts,
							 int shard_id,
							 int shard_count)
{
	MemSet(task, 0, sizeof(*task));
	task->role = (int) role;
	task->status = FB_PARALLEL_EXPORT_STATUS_INIT;
	task->dboid = MyDatabaseId;
	task->useroid = GetUserId();
	task->source_relid = source_relid;
	task->namespace_oid = namespace_oid;
	task->shard_id = shard_id;
	task->shard_count = shard_count;

	if (strlen(target_ts) >= sizeof(task->target_ts))
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("target timestamp text is too long for parallel export task")));

	strlcpy(task->target_ts, target_ts, sizeof(task->target_ts));
	strlcpy(task->final_relname, final_relname, sizeof(task->final_relname));
	if (stage_relname != NULL)
		strlcpy(task->stage_relname, stage_relname, sizeof(task->stage_relname));
	fb_parallel_export_capture_gucs(task);
}

static char *
fb_parallel_stage_relname(const char *final_relname, int shard_id)
{
	char suffix[32];

	snprintf(suffix, sizeof(suffix), "fbp%d_%d", MyProcPid, shard_id);
	return makeObjectName(final_relname, suffix, NULL);
}

static bool
fb_export_slot_matches_shard(const FbRelationInfo *info,
							 TupleTableSlot *slot,
							 TupleDesc tupdesc,
							 int shard_id,
							 int shard_count)
{
	char *identity;
	uint32 hash;
	bool matched;

	if (shard_id < 0 || shard_count <= 1)
		return true;

	identity = fb_apply_build_key_identity_slot(info, slot, tupdesc);
	if (identity == NULL)
		return false;

	hash = fb_apply_hash_identity(identity);
	matched = ((int) (hash % (uint32) shard_count)) == shard_id;
	pfree(identity);
	return matched;
}

static uint64
fb_parallel_export_load_stage_shared(const FbParallelExportTask *task,
									 FbParallelExportShared *shared,
									 const FbRelationInfo *info)
{
	FbReverseOpReader *reader = NULL;
	void *apply_state = NULL;
	Relation source_rel = NULL;
	Relation stage_rel = NULL;
	TableScanDesc scan = NULL;
	TupleTableSlot *scan_slot = NULL;
	TupleTableSlot *target_slot = NULL;
	BulkInsertState bistate = NULL;
	CommandId cid;
	uint64 rowcount = 0;
	FbApplyEmit emit;

	source_rel = relation_open(task->source_relid, AccessShareLock);
	reader = fb_reverse_reader_open_shared(
		fb_parallel_export_reverse_shared_const(shared));
	apply_state = fb_keyed_apply_begin_reader(info,
											  RelationGetDescr(source_rel),
											  reader,
											  (long) Min(fb_reverse_source_shared_total_count(
												fb_parallel_export_reverse_shared_const(shared)),
											  (uint64) LONG_MAX),
											  0,
											  fb_get_memory_limit_bytes(),
											  task->shard_id,
											  task->shard_count);
	fb_reverse_reader_close(reader);
	reader = NULL;

	stage_rel = relation_open(task->created_relid, RowExclusiveLock);
	scan = table_beginscan(source_rel, GetActiveSnapshot(), 0, NULL);
	scan_slot = table_slot_create(source_rel, NULL);
	target_slot = table_slot_create(stage_rel, NULL);
	bistate = GetBulkInsertState();
	cid = GetCurrentCommandId(true);

	PG_TRY();
	{
		while (table_scan_getnextslot(scan, ForwardScanDirection, scan_slot))
		{
			if (!fb_export_slot_matches_shard(info,
											  scan_slot,
											  RelationGetDescr(source_rel),
											  task->shard_id,
											  task->shard_count))
			{
				ExecClearTuple(scan_slot);
				continue;
			}

			if (!fb_keyed_apply_process_current_emit(apply_state, scan_slot, &emit))
			{
				ExecClearTuple(scan_slot);
				continue;
			}

			if (emit.kind == FB_APPLY_EMIT_SLOT)
				ExecCopySlot(target_slot, emit.slot);
			else if (emit.kind == FB_APPLY_EMIT_TUPLE)
				ExecForceStoreHeapTuple(emit.tuple, target_slot, false);
			else
			{
				ExecClearTuple(scan_slot);
				ExecClearTuple(target_slot);
				continue;
			}

			table_tuple_insert(stage_rel,
							   target_slot,
							   cid,
							   TABLE_INSERT_SKIP_FSM,
							   bistate);
			ExecClearTuple(scan_slot);
			ExecClearTuple(target_slot);
			rowcount++;
		}

		fb_keyed_apply_finish_scan(apply_state);
		while (fb_keyed_apply_next_residual_emit(apply_state, &emit))
		{
			if (emit.kind != FB_APPLY_EMIT_TUPLE)
				continue;

			ExecForceStoreHeapTuple(emit.tuple, target_slot, false);
			table_tuple_insert(stage_rel,
							   target_slot,
							   cid,
							   TABLE_INSERT_SKIP_FSM,
							   bistate);
			ExecClearTuple(target_slot);
			rowcount++;
		}

		table_finish_bulk_insert(stage_rel, TABLE_INSERT_SKIP_FSM);
		FreeBulkInsertState(bistate);
		bistate = NULL;
		ExecDropSingleTupleTableSlot(scan_slot);
		ExecDropSingleTupleTableSlot(target_slot);
		scan_slot = NULL;
		target_slot = NULL;
		table_endscan(scan);
		scan = NULL;
		relation_close(stage_rel, RowExclusiveLock);
		stage_rel = NULL;
		relation_close(source_rel, AccessShareLock);
		source_rel = NULL;
		fb_keyed_apply_end(apply_state);
		apply_state = NULL;
	}
	PG_CATCH();
	{
		if (bistate != NULL)
			FreeBulkInsertState(bistate);
		if (scan_slot != NULL)
			ExecDropSingleTupleTableSlot(scan_slot);
		if (target_slot != NULL)
			ExecDropSingleTupleTableSlot(target_slot);
		if (scan != NULL)
			table_endscan(scan);
		if (stage_rel != NULL)
			relation_close(stage_rel, RowExclusiveLock);
		if (source_rel != NULL)
			relation_close(source_rel, AccessShareLock);
		if (apply_state != NULL)
			fb_keyed_apply_end(apply_state);
		if (reader != NULL)
			fb_reverse_reader_close(reader);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return rowcount;
}

static void
fb_export_drop_table_if_exists(Oid namespace_oid, const char *relname)
{
	char *qualified_name;
	char *sql;

	if (relname == NULL || relname[0] == '\0')
		return;

	qualified_name = fb_export_qualified_relation_name(namespace_oid, relname);
	sql = psprintf("DROP TABLE IF EXISTS %s", qualified_name);
	fb_export_exec_ddl(sql);
}

static void
fb_export_drop_created_table(FbParallelExportTask *task, const char *relname)
{
	Oid relid;

	if (task == NULL || !OidIsValid(task->created_relid))
		return;
	if (relname == NULL || relname[0] == '\0')
		return;

	relid = get_relname_relid(relname, task->namespace_oid);
	if (relid != task->created_relid)
		return;

	fb_export_drop_table_if_exists(task->namespace_oid, relname);
}

static void
fb_parallel_export_worker_do(FbParallelExportTask *task,
							 FbParallelExportShared *shared)
{
	FbRelationInfo info;
	FbParallelExportTask *tasks = fb_parallel_export_tasks(shared);

	fb_catalog_load_relation_info(task->source_relid, &info);

	if (task->role == FB_PARALLEL_EXPORT_ROLE_PREPARE)
	{
		FbFlashbackExportTarget target;
		int spi_rc;

		MemSet(&target, 0, sizeof(target));
		target.namespace_oid = task->namespace_oid;
		target.relname = pstrdup(task->final_relname);

		spi_rc = SPI_connect();
		if (spi_rc != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed: %d", spi_rc);
		fb_export_create_base_table(task->source_relid, &target);
		task->created_relid = target.relid;
		SPI_finish();
		return;
	}

	if (task->role == FB_PARALLEL_EXPORT_ROLE_SHARD)
	{
		if (!OidIsValid(task->created_relid))
			elog(ERROR, "could not resolve flashback export target relation");
		task->rowcount = fb_parallel_export_load_stage_shared(task, shared, &info);
		return;
	}

	if (task->role == FB_PARALLEL_EXPORT_ROLE_FINALIZE)
	{
		int spi_rc;
		Oid target_relid;
		FbFlashbackExportTarget target;
		int i;

		target_relid = get_relname_relid(task->final_relname, task->namespace_oid);
		if (!OidIsValid(target_relid))
			elog(ERROR, "could not resolve flashback export target relation");
		MemSet(&target, 0, sizeof(target));
		target.namespace_oid = task->namespace_oid;
		target.relid = target_relid;
		target.relname = pstrdup(task->final_relname);
		target.qualified_name = fb_export_qualified_relation_name(task->namespace_oid,
																 task->final_relname);

		spi_rc = SPI_connect();
		if (spi_rc != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed: %d", spi_rc);
		task->created_relid = target_relid;
		fb_export_add_pk_unique_constraints(task->source_relid, &target);
		fb_export_add_secondary_indexes(task->source_relid, &target);
		for (i = 0; i < shared->task_count; i++)
		{
			FbParallelExportTask *worker = &tasks[i];

			if (worker->role == FB_PARALLEL_EXPORT_ROLE_SHARD)
				task->rowcount += worker->rowcount;
		}
		SPI_finish();
		return;
	}

	if (task->role == FB_PARALLEL_EXPORT_ROLE_CLEANUP)
	{
		int spi_rc;
		int i;

		spi_rc = SPI_connect();
		if (spi_rc != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed: %d", spi_rc);
		for (i = 0; i < shared->task_count; i++)
		{
			FbParallelExportTask *worker = &tasks[i];

			if (worker->role == FB_PARALLEL_EXPORT_ROLE_PREPARE ||
				worker->role == FB_PARALLEL_EXPORT_ROLE_FINALIZE)
			{
				fb_export_drop_created_table(worker, worker->final_relname);
				break;
			}
		}
		SPI_finish();
		return;
	}

	elog(ERROR, "invalid parallel export task role: %d", task->role);
}

void
fb_parallel_export_worker_main(Datum main_arg)
{
	dsm_handle handle = DatumGetUInt32(main_arg);
	dsm_segment *seg;
	FbParallelExportShared *shared;
	int task_index;
	FbParallelExportTask *task;

	memcpy(&task_index, MyBgworkerEntry->bgw_extra, sizeof(task_index));
	BackgroundWorkerUnblockSignals();

	seg = dsm_attach(handle);
	if (seg == NULL)
		proc_exit(1);

	shared = (FbParallelExportShared *) dsm_segment_address(seg);
	task = &fb_parallel_export_tasks(shared)[task_index];
	task->status = FB_PARALLEL_EXPORT_STATUS_RUNNING;

	BackgroundWorkerInitializeConnectionByOid(task->dboid, task->useroid, 0);
	fb_parallel_export_apply_gucs(task);

	PG_TRY();
	{
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		fb_parallel_export_worker_do(task, shared);
		PopActiveSnapshot();
		CommitTransactionCommand();
		task->status = FB_PARALLEL_EXPORT_STATUS_DONE;
	}
	PG_CATCH();
	{
		ErrorData *edata = CopyErrorData();

		FlushErrorState();
		if (ActiveSnapshotSet())
			PopActiveSnapshot();
		if (IsTransactionState())
			AbortCurrentTransaction();
		strlcpy(task->errmsg, edata->message, sizeof(task->errmsg));
		task->status = FB_PARALLEL_EXPORT_STATUS_ERROR;
	}
	PG_END_TRY();

	dsm_detach(seg);
	proc_exit(0);
}

static void
fb_parallel_export_launch_worker(dsm_handle handle,
								 int task_index,
								 const char *worker_name,
								 BackgroundWorkerHandle **handle_out)
{
	BackgroundWorker worker;
	pid_t pid;
	BgwHandleStatus status;

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_flashback export");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "pg_flashback");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "fb_parallel_export_worker_main");
	worker.bgw_main_arg = UInt32GetDatum(handle);
	memcpy(worker.bgw_extra, &task_index, sizeof(task_index));
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, handle_out))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register pg_flashback export worker")));

	status = WaitForBackgroundWorkerStartup(*handle_out, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("pg_flashback export worker failed to start")));
}

static void
fb_parallel_export_wait_worker(BackgroundWorkerHandle *handle,
							   const FbParallelExportTask *task)
{
	BgwHandleStatus status;

	status = WaitForBackgroundWorkerShutdown(handle);
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("postmaster died while waiting for pg_flashback export worker")));
	if (task->status != FB_PARALLEL_EXPORT_STATUS_DONE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_flashback export worker failed"),
				 errdetail_internal("%s",
									task->errmsg[0] != '\0' ? task->errmsg : "worker exited without success")));
}

static Oid
fb_export_parallel_run(Oid source_relid,
					   text *target_ts_text,
					   const FbRelationInfo *info)
{
	int worker_count = fb_export_parallel_workers();
	Oid namespace_oid;
	char *target_relname;
	char *target_ts_cstr;
	FbFlashbackReverseBuildState build_state;
	Size tasks_size;
	Size reverse_shared_size;
	Size shared_size;
	dsm_segment *seg;
	dsm_handle seg_handle;
	FbParallelExportShared *shared;
	FbParallelExportTask *tasks;
	BackgroundWorkerHandle *handles[FB_EXPORT_PARALLEL_MAX_WORKERS + 3];
	int prepare_index;
	int finalizer_index;
	int cleanup_index;
	int i;

	if (worker_count <= 1 || worker_count > FB_EXPORT_PARALLEL_MAX_WORKERS)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid pg_flashback.export_parallel_workers value")));
	if (info == NULL || info->mode != FB_APPLY_KEYED || info->key_natts != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parallel pg_flashback_to currently requires a keyed relation with a single stable key")));

	target_relname = fb_export_target_relname(source_relid, &namespace_oid);
	target_ts_cstr = text_to_cstring(target_ts_text);
	MemSet(&build_state, 0, sizeof(build_state));

	prepare_index = worker_count;
	finalizer_index = worker_count + 1;
	cleanup_index = worker_count + 2;
	fb_export_ensure_target_absent(namespace_oid, target_relname);
	fb_flashback_build_reverse_state(source_relid, target_ts_text, true, &build_state);

	tasks_size = MAXALIGN(sizeof(FbParallelExportTask) * (worker_count + 3));
	reverse_shared_size = MAXALIGN(fb_reverse_source_shared_size(build_state.reverse));
	shared_size = MAXALIGN(sizeof(FbParallelExportShared)) +
		tasks_size +
		reverse_shared_size;
	seg = dsm_create(shared_size, 0);
	seg_handle = dsm_segment_handle(seg);
	shared = (FbParallelExportShared *) dsm_segment_address(seg);
	MemSet(shared, 0, shared_size);
	shared->task_count = worker_count + 3;
	shared->worker_count = worker_count;
	shared->tasks_offset = MAXALIGN(sizeof(FbParallelExportShared));
	shared->reverse_shared_offset = shared->tasks_offset + tasks_size;
	shared->reverse_shared_size = reverse_shared_size;
	shared->scan_shared_offset = shared->reverse_shared_offset + reverse_shared_size;
	shared->scan_shared_size = 0;
	tasks = fb_parallel_export_tasks(shared);
	fb_reverse_source_write_shared(build_state.reverse,
								   fb_parallel_export_reverse_shared(shared),
								   reverse_shared_size);

	fb_parallel_export_fill_task(&tasks[prepare_index],
								 FB_PARALLEL_EXPORT_ROLE_PREPARE,
								 source_relid,
								 namespace_oid,
								 target_relname,
								 NULL,
								 target_ts_cstr,
								 -1,
								 worker_count);
	fb_parallel_export_launch_worker(seg_handle,
									 prepare_index,
									 "pg_flashback export prepare",
									 &handles[prepare_index]);
	fb_parallel_export_wait_worker(handles[prepare_index], &tasks[prepare_index]);

	for (i = 0; i < worker_count; i++)
	{
		char *stage_relname = fb_parallel_stage_relname(target_relname, i);
		char worker_name[BGW_MAXLEN];

		fb_parallel_export_fill_task(&tasks[i],
									 FB_PARALLEL_EXPORT_ROLE_SHARD,
									 source_relid,
									 namespace_oid,
									 target_relname,
										 stage_relname,
										 target_ts_cstr,
										 i,
										 worker_count);
		tasks[i].created_relid = tasks[prepare_index].created_relid;
		snprintf(worker_name, sizeof(worker_name),
				 "pg_flashback export shard %d/%d",
				 i + 1,
				 worker_count);
		fb_parallel_export_launch_worker(seg_handle, i, worker_name, &handles[i]);
	}

	fb_parallel_export_fill_task(&tasks[finalizer_index],
								 FB_PARALLEL_EXPORT_ROLE_FINALIZE,
								 source_relid,
								 namespace_oid,
								 target_relname,
								 NULL,
								 target_ts_cstr,
								 -1,
								 worker_count);
		fb_parallel_export_fill_task(&tasks[cleanup_index],
									 FB_PARALLEL_EXPORT_ROLE_CLEANUP,
								 source_relid,
								 namespace_oid,
								 target_relname,
								 NULL,
								 target_ts_cstr,
								 -1,
								 worker_count);

	PG_TRY();
		{
			for (i = 0; i < worker_count; i++)
				fb_parallel_export_wait_worker(handles[i], &tasks[i]);

		fb_parallel_export_launch_worker(seg_handle,
										 finalizer_index,
										 "pg_flashback export finalizer",
										 &handles[finalizer_index]);
		fb_parallel_export_wait_worker(handles[finalizer_index],
									   &tasks[finalizer_index]);
	}
	PG_CATCH();
	{
		BackgroundWorkerHandle *cleanup_handle = NULL;

		fb_parallel_export_launch_worker(seg_handle,
										 cleanup_index,
										 "pg_flashback export cleanup",
										 &cleanup_handle);
		WaitForBackgroundWorkerShutdown(cleanup_handle);
		fb_flashback_release_reverse_state(&build_state);
		dsm_detach(seg);
		fb_progress_abort();
		PG_RE_THROW();
	}
	PG_END_TRY();

	source_relid = tasks[prepare_index].created_relid;
	fb_flashback_release_reverse_state(&build_state);
	dsm_detach(seg);
	fb_progress_finish();
	return source_relid;
}

Datum
pg_flashback_rewind(PG_FUNCTION_ARGS)
{
	Oid source_relid = PG_GETARG_OID(0);
	text *target_ts_text = PG_GETARG_TEXT_PP(1);
	FbRelationInfo info;
	FbFlashbackReverseBuildState build_state;
	void *apply_state = NULL;
	FbKeyedResidualItem *upsert_items = NULL;
	FbKeyedDeleteKey *delete_items = NULL;
	uint64 upsert_count = 0;
	uint64 delete_count = 0;
	uint64 affected = 0;
	char *upsert_relname = NULL;
	char *delete_relname = NULL;
	AttrNumber key_attnum;
	Form_pg_attribute key_attr;
	Oid upsert_relid = InvalidOid;
	Oid delete_relid = InvalidOid;
	int spi_rc;
	bool spi_connected = false;

	LockRelationOid(source_relid, AccessExclusiveLock);
	fb_catalog_load_relation_info(source_relid, &info);
	MemSet(&build_state, 0, sizeof(build_state));

	PG_TRY();
	{
		fb_flashback_build_reverse_state(source_relid, target_ts_text, false, &build_state);
		fb_rewind_require_supported(source_relid, &info, build_state.tupdesc);

		apply_state = fb_keyed_apply_begin(&info, build_state.tupdesc, build_state.reverse);
		if (!fb_keyed_apply_supports_single_typed_key(apply_state))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("pg_flashback_to currently requires a single typed stable key")));

		upsert_items = fb_keyed_apply_collect_residual_items(apply_state, &upsert_count);
		delete_items = fb_keyed_apply_collect_delete_keys(apply_state, &delete_count);
		fb_flashback_release_reverse_state(&build_state);

		spi_rc = SPI_connect();
		if (spi_rc != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed: %d", spi_rc);
		spi_connected = true;

		upsert_relname = fb_rewind_temp_relname("fbrew_upsert");
		delete_relname = fb_rewind_temp_relname("fbrew_delete");
		key_attnum = info.key_attnums[0];
		key_attr = TupleDescAttr(build_state.tupdesc, key_attnum - 1);
		upsert_relid = fb_rewind_create_upsert_table(source_relid, upsert_relname);
		delete_relid = fb_rewind_create_delete_table(delete_relname,
													 NameStr(key_attr->attname),
													 key_attr->atttypid,
													 key_attr->atttypmod);

		fb_rewind_load_upsert_rows(upsert_relid, upsert_items, upsert_count);
		fb_rewind_load_delete_keys(delete_relid, delete_items, delete_count);
		affected = fb_rewind_apply_source_dml(source_relid,
											  upsert_relname,
											  delete_relname,
											  build_state.tupdesc,
											  key_attnum);

		SPI_finish();
		spi_connected = false;
		fb_keyed_apply_end(apply_state);
		apply_state = NULL;
		fb_progress_finish();
	}
	PG_CATCH();
	{
		if (spi_connected)
			SPI_finish();
		if (apply_state != NULL)
			fb_keyed_apply_end(apply_state);
		fb_flashback_release_reverse_state(&build_state);
		fb_progress_abort();
		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_RETURN_INT64((int64) affected);
}

Datum
pg_flashback_to(PG_FUNCTION_ARGS)
{
	Oid source_relid = PG_GETARG_OID(0);
	text *target_ts_text = PG_GETARG_TEXT_PP(1);
	FbRelationInfo info;
	FbFlashbackExportTarget target;
	int spi_rc;

	fb_catalog_load_relation_info(source_relid, &info);

	if (fb_export_parallel_supported(&info))
		PG_RETURN_OID(fb_export_parallel_run(source_relid,
											 target_ts_text,
											 &info));

	MemSet(&target, 0, sizeof(target));
	target.relname = fb_export_target_relname(source_relid, &target.namespace_oid);

	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %d", spi_rc);

	PG_TRY();
	{
		fb_export_create_base_table(source_relid, &target);
		SPI_finish();
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	fb_export_load_rows(source_relid, target.relid, target_ts_text);

	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %d", spi_rc);

	PG_TRY();
	{
		fb_export_add_pk_unique_constraints(source_relid, &target);
		fb_export_add_secondary_indexes(source_relid, &target);
		SPI_finish();
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_RETURN_OID(target.relid);
}
