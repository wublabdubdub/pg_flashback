#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "access/transam.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#include "fb_catalog.h"
#include "fb_compat.h"
#include "fb_guc.h"
#include "fb_runtime.h"
#include "summaryd/fb_summaryd_core.h"

#define SUMMARYD_PG_EPOCH_UNIX INT64CONST(946684800)

char *DataDir = NULL;
int MyProcPid = 0;
MemoryContext CurrentMemoryContext = NULL;
MemoryContext TopMemoryContext = NULL;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
TransamVariablesData *TransamVariables = NULL;

static char summaryd_shim_archive_dest[SUMMARYD_PATH_MAX];
static char summaryd_shim_pg_wal_dir[SUMMARYD_PATH_MAX];
static char summaryd_shim_recovered_wal_dir[SUMMARYD_PATH_MAX];
static char summaryd_shim_meta_dir[SUMMARYD_PATH_MAX];
static char summaryd_shim_meta_summary_dir[SUMMARYD_PATH_MAX];
static char summaryd_shim_meta_summaryd_dir[SUMMARYD_PATH_MAX];
static char summaryd_shim_runtime_dir[SUMMARYD_PATH_MAX];
static char summaryd_shim_runtime_hint_dir[SUMMARYD_PATH_MAX];
static char summaryd_shim_state_path[SUMMARYD_PATH_MAX];
static char summaryd_shim_debug_path[SUMMARYD_PATH_MAX];
static char summaryd_shim_last_query_hint_path[SUMMARYD_PATH_MAX];
static char summaryd_shim_last_error[1024];

static void
summaryd_shim_copy_path(char *dst, size_t dst_len, const char *src)
{
	if (dst == NULL || dst_len == 0)
		return;
	if (src == NULL)
		src = "";
	strlcpy(dst, src, dst_len);
}

static void
summaryd_shim_format_path(char *dst, size_t dst_len, const char *base, const char *suffix)
{
	int written;

	written = snprintf(dst, dst_len, "%s/%s", base, suffix);
	if (written < 0 || written >= (int) dst_len)
	{
		fprintf(stderr, "summaryd shim path too long: %s/%s\n", base, suffix);
		abort();
	}
}

static void
summaryd_shim_set_error_va(const char *fmt, va_list args)
{
	if (fmt == NULL)
	{
		summaryd_shim_last_error[0] = '\0';
		return;
	}

	vsnprintf(summaryd_shim_last_error,
			  sizeof(summaryd_shim_last_error),
			  fmt,
			  args);
}

const char *
summaryd_core_last_error(void)
{
	return summaryd_shim_last_error;
}

void
summaryd_core_clear_error(void)
{
	summaryd_shim_last_error[0] = '\0';
}

void
summaryd_core_set_paths(const SummarydConfig *config)
{
	if (config == NULL)
		return;

	DataDir = config->pgdata;
	MyProcPid = getpid();
	CurrentMemoryContext = (MemoryContext) &CurrentMemoryContext;
	TopMemoryContext = (MemoryContext) &TopMemoryContext;

	summaryd_shim_copy_path(summaryd_shim_archive_dest,
							 sizeof(summaryd_shim_archive_dest),
							 config->archive_dest);
	summaryd_shim_format_path(summaryd_shim_pg_wal_dir,
							  sizeof(summaryd_shim_pg_wal_dir),
							  config->pgdata,
							  "pg_wal");
	summaryd_shim_format_path(summaryd_shim_recovered_wal_dir,
							  sizeof(summaryd_shim_recovered_wal_dir),
							  config->pgdata,
							  "pg_flashback/recovered_wal");
	summaryd_shim_format_path(summaryd_shim_meta_dir,
							  sizeof(summaryd_shim_meta_dir),
							  config->pgdata,
							  "pg_flashback/meta");
	summaryd_shim_format_path(summaryd_shim_meta_summary_dir,
							  sizeof(summaryd_shim_meta_summary_dir),
							  config->pgdata,
							  "pg_flashback/meta/summary");
	summaryd_shim_format_path(summaryd_shim_meta_summaryd_dir,
							  sizeof(summaryd_shim_meta_summaryd_dir),
							  config->pgdata,
							  "pg_flashback/meta/summaryd");
	summaryd_shim_format_path(summaryd_shim_runtime_dir,
							  sizeof(summaryd_shim_runtime_dir),
							  config->pgdata,
							  "pg_flashback/runtime");
	summaryd_shim_format_path(summaryd_shim_runtime_hint_dir,
							  sizeof(summaryd_shim_runtime_hint_dir),
							  config->pgdata,
							  "pg_flashback/runtime/summary-hints");
	summaryd_shim_format_path(summaryd_shim_state_path,
							  sizeof(summaryd_shim_state_path),
							  config->pgdata,
							  "pg_flashback/meta/summaryd/state.json");
	summaryd_shim_format_path(summaryd_shim_debug_path,
							  sizeof(summaryd_shim_debug_path),
							  config->pgdata,
							  "pg_flashback/meta/summaryd/debug.json");
	summaryd_shim_format_path(summaryd_shim_last_query_hint_path,
							  sizeof(summaryd_shim_last_query_hint_path),
							  config->pgdata,
							  "pg_flashback/runtime/summary-hints/last-query.json");
	summaryd_core_clear_error();
}

void *
MemoryContextAlloc(MemoryContext context, Size size)
{
	(void) context;
	return palloc(size);
}

void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	(void) context;
	return palloc0(size);
}

void *
MemoryContextAllocExtended(MemoryContext context, Size size, int flags)
{
	(void) context;
	return palloc_extended(size, flags);
}

MemoryContext
AllocSetContextCreateInternal(MemoryContext parent,
							  const char *name,
							  Size minContextSize,
							  Size initBlockSize,
							  Size maxBlockSize)
{
	(void) parent;
	(void) name;
	(void) minContextSize;
	(void) initBlockSize;
	(void) maxBlockSize;
	return (MemoryContext) palloc0(1);
}

void
MemoryContextDelete(MemoryContext context)
{
	pfree(context);
}

void
MemoryContextSetIdentifier(MemoryContext context, const char *id)
{
	(void) context;
	(void) id;
}

Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

int
GetCurrentTransactionNestLevel(void)
{
	return 1;
}

int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	(void) lock;
	(void) file;
	(void) line;
	(void) func;
	return 0;
}

DIR *
AllocateDir(const char *dirname)
{
	return opendir(dirname);
}

struct dirent *
ReadDir(DIR *dir, const char *dirname)
{
	(void) dirname;
	errno = 0;
	return readdir(dir);
}

struct dirent *
ReadDirExtended(DIR *dir, const char *dirname, int elevel)
{
	(void) elevel;
	return ReadDir(dir, dirname);
}

int
FreeDir(DIR *dir)
{
	return closedir(dir);
}

FILE *
AllocateFile(const char *name, const char *mode)
{
	return fopen(name, mode);
}

int
FreeFile(FILE *file)
{
	return fclose(file);
}

int
OpenTransientFile(const char *fileName, int fileFlags)
{
	return open(fileName, fileFlags, 0600);
}

int
OpenTransientFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	return open(fileName, fileFlags, fileMode);
}

int
CloseTransientFile(int fd)
{
	return close(fd);
}

TimestampTz
GetCurrentTimestamp(void)
{
	time_t now = time(NULL);

	return ((TimestampTz) ((int64) now - SUMMARYD_PG_EPOCH_UNIX)) * USECS_PER_SEC;
}

bool
errstart(int elevel, const char *domain)
{
	(void) elevel;
	(void) domain;
	summaryd_core_clear_error();
	return true;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename, int lineno, const char *funcname)
{
	if (summaryd_shim_last_error[0] == '\0')
	{
		snprintf(summaryd_shim_last_error,
				 sizeof(summaryd_shim_last_error),
				 "summaryd standalone error at %s:%d %s",
				 filename,
				 lineno,
				 funcname != NULL ? funcname : "");
	}

	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);

	fprintf(stderr, "%s\n", summaryd_shim_last_error);
	abort();
}

int
errcode(int sqlerrcode)
{
	return sqlerrcode;
}

int
errcode_for_file_access(void)
{
	return 0;
}

int
errmsg(const char *fmt,...)
{
	va_list args;

	va_start(args, fmt);
	summaryd_shim_set_error_va(fmt, args);
	va_end(args);
	return 0;
}

int
errmsg_internal(const char *fmt,...)
{
	va_list args;

	va_start(args, fmt);
	summaryd_shim_set_error_va(fmt, args);
	va_end(args);
	return 0;
}

int
errdetail(const char *fmt,...)
{
	(void) fmt;
	return 0;
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

char *
fb_try_resolve_archive_dir(FbArchiveDirSource *source_out,
						   const char **setting_name_out)
{
	if (source_out != NULL)
		*source_out = FB_ARCHIVE_DIR_SOURCE_EXPLICIT_DEST;
	if (setting_name_out != NULL)
		*setting_name_out = "pg_flashback.archive_dest";
	return pstrdup(summaryd_shim_archive_dest);
}

char *
fb_get_effective_archive_dir(void)
{
	return pstrdup(summaryd_shim_archive_dest);
}

char *
fb_get_pg_wal_dir(void)
{
	return pstrdup(summaryd_shim_pg_wal_dir);
}

void
fb_runtime_ensure_initialized(void)
{
}

char *
fb_runtime_base_dir(void)
{
	return psprintf("%s/pg_flashback", DataDir);
}

char *
fb_runtime_runtime_dir(void)
{
	return pstrdup(summaryd_shim_runtime_dir);
}

char *
fb_runtime_recovered_wal_dir(void)
{
	return pstrdup(summaryd_shim_recovered_wal_dir);
}

char *
fb_runtime_meta_dir(void)
{
	return pstrdup(summaryd_shim_meta_dir);
}

char *
fb_runtime_meta_summary_dir(void)
{
	return pstrdup(summaryd_shim_meta_summary_dir);
}

char *
fb_runtime_meta_summaryd_dir(void)
{
	return pstrdup(summaryd_shim_meta_summaryd_dir);
}

char *
fb_runtime_summaryd_state_path(void)
{
	return pstrdup(summaryd_shim_state_path);
}

char *
fb_runtime_summaryd_debug_path(void)
{
	return pstrdup(summaryd_shim_debug_path);
}

char *
fb_runtime_summary_hint_dir(void)
{
	return pstrdup(summaryd_shim_runtime_hint_dir);
}

char *
fb_runtime_summary_last_query_hint_path(void)
{
	return pstrdup(summaryd_shim_last_query_hint_path);
}

void
fb_runtime_ensure_summary_daemon_dirs(void)
{
}

void
fb_runtime_cleanup_stale(void)
{
}

void
fb_runtime_cleanup_current_backend(void)
{
}

XLogRecPtr
fb_xlog_find_next_record_compat(XLogReaderState *state, XLogRecPtr recptr)
{
	return XLogFindNextRecord(state, recptr);
}

void
fb_catalog_load_relation_info(Oid relid, FbRelationInfo *info)
{
	(void) relid;
	(void) info;
	abort();
}

bool
fb_catalog_relation_creation_precedes_target(Oid relid, TimestampTz target_ts)
{
	(void) relid;
	(void) target_ts;
	return false;
}
