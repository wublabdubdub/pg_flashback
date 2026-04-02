/*
 * fb_guc.c
 *    GUC definitions and runtime configuration selection.
 */

#include "postgres.h"

#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"

#include "fb_guc.h"
#include "fb_custom_scan.h"
#include "fb_runtime.h"
#include "fb_summary_service.h"

static char *fb_archive_dir = NULL;
static char *fb_archive_dest = NULL;
static char *fb_debug_pg_wal_dir = NULL;
static char *fb_memory_limit_setting = NULL;
static char *fb_spill_mode_setting = NULL;
static uint64 fb_memory_limit_kb = UINT64CONST(1048576);
static FbSpillMode fb_spill_mode = FB_SPILL_MODE_AUTO;
static int	fb_parallel_workers_setting = 8;
static bool fb_show_progress = true;
static bool fb_summary_service_setting = true;
static int fb_summary_service_workers_setting = 2;
static int fb_summary_service_scan_interval_ms_setting = 1000;
static int fb_summary_service_queue_size_setting = 512;
static int fb_summary_service_meta_limit_mb_setting = 256;
static int fb_summary_service_meta_low_watermark_percent_setting = 80;

#define FB_MEMORY_LIMIT_GUC_NAME "pg_flashback.memory_limit"
#define FB_SPILL_MODE_GUC_NAME "pg_flashback.spill_mode"
#define FB_MEMORY_LIMIT_MIN_KB UINT64CONST(1)
#define FB_MEMORY_LIMIT_MAX_KB (UINT64CONST(32) * UINT64CONST(1024) * UINT64CONST(1024))

typedef struct FbMemoryLimitParseResult
{
	uint64 kb;
} FbMemoryLimitParseResult;

typedef struct FbSpillModeParseResult
{
	FbSpillMode mode;
} FbSpillModeParseResult;

static bool fb_is_blank_string(const char *value);
static char *fb_trim_trailing_slashes(const char *path);
static bool fb_parse_spill_mode(const char *value,
								FbSpillMode *mode_out,
								const char **detail_out,
								const char **canonical_out);
static bool fb_memory_limit_unit_multiplier(const char *unit,
											uint64 *multiplier_kb,
											bool *unit_is_bytes);
static bool fb_memory_limit_parse_kb(const char *value,
									 uint64 *kb_out,
									 const char **detail_out);
static char *fb_memory_limit_canonical_text(uint64 kb);
static bool fb_spill_mode_check_hook(char **newval, void **extra, GucSource source);
static void fb_spill_mode_assign_hook(const char *newval, void *extra);
static bool fb_memory_limit_check_hook(char **newval, void **extra, GucSource source);
static void fb_memory_limit_assign_hook(const char *newval, void *extra);
static const char *fb_token_basename(const char *token);
static bool fb_token_matches_command(const char *token, const char *command_name);
static List *fb_tokenize_shell_command(const char *command);
static char *fb_archive_dir_from_percent_f_path(const char *token);
static char *fb_try_parse_cp_archive_dir(List *tokens);
static char *fb_try_parse_pg_probackup_archive_dir(List *tokens);
static char *fb_try_parse_archive_command_dir(const char *command);
static char *fb_get_core_config_string(const char *name);
static void fb_raise_archive_autodiscovery_error(const char *detail);
static void fb_require_existing_directory(const char *guc_name, const char *path);
static char *fb_resolve_archive_dir_internal(FbArchiveDirSource *source_out,
											 const char **setting_name_out,
											 bool error_if_missing);

/*
 * _PG_init
 *    GUC entry point.
 */

PGDLLEXPORT void _PG_init(void);

/*
 * _PG_init
 *    GUC entry point.
 */

void
_PG_init(void)
{
	DefineCustomStringVariable("pg_flashback.archive_dir",
							   "Legacy single-directory WAL source for pg_flashback.",
							   NULL,
							   &fb_archive_dir,
							   NULL,
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pg_flashback.archive_dest",
							   "Primary archive destination for pg_flashback historical WAL reads.",
							   NULL,
							   &fb_archive_dest,
							   NULL,
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pg_flashback.debug_pg_wal_dir",
							   "Development-only override for the pg_wal directory used by pg_flashback.",
							   NULL,
							   &fb_debug_pg_wal_dir,
							   NULL,
							   PGC_USERSET,
							   GUC_NOT_IN_SAMPLE,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable(FB_MEMORY_LIMIT_GUC_NAME,
							   "Per-query memory cap for tracked pg_flashback hot-path structures.",
							   "Limits tracked RecordRef/FPI/block-data/main-data and BlockReplayStore bytes. Accepts case-insensitive memory units such as kb, mb, and gb.",
							   &fb_memory_limit_setting,
							   "1GB",
							   PGC_USERSET,
							   0,
							   fb_memory_limit_check_hook,
							   fb_memory_limit_assign_hook,
							   NULL);

	DefineCustomStringVariable(FB_SPILL_MODE_GUC_NAME,
							   "Controls whether pg_flashback may continue with disk-backed spill.",
							   "auto fails early when the estimated working set exceeds pg_flashback.memory_limit; memory enforces in-memory execution; disk allows disk-backed spill to continue.",
							   &fb_spill_mode_setting,
							   "auto",
							   PGC_USERSET,
							   0,
							   fb_spill_mode_check_hook,
							   fb_spill_mode_assign_hook,
							   NULL);

	DefineCustomIntVariable("pg_flashback.parallel_workers",
							"Control flashback-stage parallel worker count where the implementation supports parallel execution.",
							"When 0, the flashback pipeline stays serial. When greater than 0, pg_flashback may run safe flashback stages in parallel up to the configured worker count.",
							&fb_parallel_workers_setting,
							8,
							0,
							16,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_flashback.show_progress",
							 "Show pg_flashback stage progress via NOTICE messages.",
							 "When on, pg_flashback emits client-visible stage progress and percentages for long-running flashback steps.",
							 &fb_show_progress,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_flashback.summary_service",
							 "Enable pg_flashback background summary prebuild service.",
							 "When on and pg_flashback is loaded via shared_preload_libraries, launcher/workers prebuild WAL segment summaries in the background.",
							 &fb_summary_service_setting,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_flashback.summary_workers",
							"Background worker count for summary prebuild.",
							"Controls how many background summary builder workers run when pg_flashback.summary_service is enabled.",
							&fb_summary_service_workers_setting,
							2,
							1,
							8,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_flashback.summary_scan_interval_ms",
							"Directory rescan interval for background summary prebuild.",
							"Controls how often the summary launcher rescans archive and pg_wal directories for missing summaries.",
							&fb_summary_service_scan_interval_ms_setting,
							1000,
							100,
							60000,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_flashback.summary_queue_size",
							"Queue slot count for background summary prebuild tasks.",
							"Fixed shared-memory queue capacity for launcher-to-worker summary build tasks.",
							&fb_summary_service_queue_size_setting,
							512,
							64,
							4096,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_flashback.summary_meta_limit_mb",
							"Soft size limit for meta/summary cleanup.",
							"When meta/summary grows beyond this many megabytes, the launcher deletes older regenerable summary files down to the low watermark.",
							&fb_summary_service_meta_limit_mb_setting,
							256,
							1,
							4096,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_flashback.summary_meta_low_watermark_percent",
							"Cleanup stop point after summary meta exceeds the limit.",
							"Summary cleanup runs until meta/summary shrinks to this percentage of pg_flashback.summary_meta_limit_mb.",
							&fb_summary_service_meta_low_watermark_percent_setting,
							80,
							25,
							95,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	/*
	 * Once known pg_flashback GUCs are defined, reject any future unknown
	 * pg_flashback.* placeholders instead of silently accepting typos.
	 * Deferring this call preserves placeholders the user set before the
	 * module was loaded and lets DefineCustom* adopt their values.
	 */
	MarkGUCPrefixReserved("pg_flashback");

	fb_runtime_ensure_initialized();
	fb_custom_scan_init();
	if (process_shared_preload_libraries_in_progress)
		fb_summary_service_shmem_init();
}

/*
 * fb_get_archive_dir
 *    GUC entry point.
 */

const char *
fb_get_archive_dir(void)
{
	return fb_archive_dir;
}

/*
 * fb_get_archive_dest
 *    GUC entry point.
 */

const char *
fb_get_archive_dest(void)
{
	return fb_archive_dest;
}

/*
 * fb_get_effective_archive_dir
 *    GUC entry point.
 */

char *
fb_get_effective_archive_dir(void)
{
	return fb_resolve_archive_dir(NULL, NULL);
}

/*
 * fb_resolve_archive_dir
 *    GUC entry point.
 */

char *
fb_resolve_archive_dir(FbArchiveDirSource *source_out,
						 const char **setting_name_out)
{
	return fb_resolve_archive_dir_internal(source_out, setting_name_out, true);
}

char *
fb_try_resolve_archive_dir(FbArchiveDirSource *source_out,
							  const char **setting_name_out)
{
	return fb_resolve_archive_dir_internal(source_out, setting_name_out, false);
}

/*
 * fb_get_archive_dir_source
 *    GUC entry point.
 */

FbArchiveDirSource
fb_get_archive_dir_source(void)
{
	FbArchiveDirSource source = FB_ARCHIVE_DIR_SOURCE_NONE;
	char *path;

	path = fb_resolve_archive_dir_internal(&source, NULL, false);
	if (path != NULL)
		pfree(path);

	return source;
}

/*
 * fb_get_archive_dir_setting_name
 *    GUC entry point.
 */

const char *
fb_get_archive_dir_setting_name(void)
{
	const char *setting_name = NULL;
	char *path;

	path = fb_resolve_archive_dir_internal(NULL, &setting_name, false);
	if (path != NULL)
		pfree(path);

	return setting_name;
}

/*
 * fb_archive_dir_source_name
 *    GUC entry point.
 */

const char *
fb_archive_dir_source_name(FbArchiveDirSource source)
{
	switch (source)
	{
		case FB_ARCHIVE_DIR_SOURCE_EXPLICIT_DEST:
			return "archive_dest";
		case FB_ARCHIVE_DIR_SOURCE_LEGACY_DIR:
			return "archive_dir";
		case FB_ARCHIVE_DIR_SOURCE_ARCHIVE_COMMAND:
			return "archive_command";
		case FB_ARCHIVE_DIR_SOURCE_NONE:
		default:
			return "none";
	}
}

/*
 * fb_using_legacy_archive_dir
 *    GUC entry point.
 */

bool
fb_using_legacy_archive_dir(void)
{
	return fb_get_archive_dir_source() == FB_ARCHIVE_DIR_SOURCE_LEGACY_DIR;
}

/*
 * fb_get_pg_wal_dir
 *    GUC entry point.
 */

char *
fb_get_pg_wal_dir(void)
{
	if (fb_debug_pg_wal_dir != NULL && fb_debug_pg_wal_dir[0] != '\0')
		return pstrdup(fb_debug_pg_wal_dir);

	return psprintf("%s/pg_wal", DataDir);
}

/*
 * fb_require_existing_directory
 *    GUC helper.
 */

static void
fb_require_existing_directory(const char *guc_name, const char *path)
{
	struct stat st;

	if (path == NULL || path[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("%s is not set", guc_name)));

	if (stat(path, &st) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("%s does not exist: %s", guc_name, path)));

	if (!S_ISDIR(st.st_mode))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("%s is not a directory: %s", guc_name, path)));
}

/*
 * fb_require_archive_dir
 *    GUC entry point.
 */

void
fb_require_archive_dir(void)
{
	FbArchiveDirSource source = FB_ARCHIVE_DIR_SOURCE_NONE;
	const char *setting_name = NULL;
	char *archive_dir;

	archive_dir = fb_resolve_archive_dir(&source, &setting_name);
	fb_require_existing_directory(setting_name, archive_dir);
	pfree(archive_dir);
}

/*
 * fb_get_memory_limit_bytes
 *    GUC entry point.
 */

uint64
fb_get_memory_limit_bytes(void)
{
	return fb_memory_limit_kb * UINT64CONST(1024);
}

FbSpillMode
fb_get_spill_mode(void)
{
	return fb_spill_mode;
}

const char *
fb_spill_mode_name(FbSpillMode mode)
{
	switch (mode)
	{
		case FB_SPILL_MODE_MEMORY:
			return "memory";
		case FB_SPILL_MODE_DISK:
			return "disk";
		case FB_SPILL_MODE_AUTO:
		default:
			return "auto";
	}
}

int
fb_parallel_workers(void)
{
	return fb_parallel_workers_setting;
}

bool
fb_summary_service_enabled(void)
{
	return fb_summary_service_setting;
}

int
fb_summary_service_workers(void)
{
	return fb_summary_service_workers_setting;
}

int
fb_summary_service_scan_interval_ms(void)
{
	return fb_summary_service_scan_interval_ms_setting;
}

int
fb_summary_service_queue_size(void)
{
	return fb_summary_service_queue_size_setting;
}

uint64
fb_summary_service_meta_limit_bytes(void)
{
	return (uint64) fb_summary_service_meta_limit_mb_setting * UINT64CONST(1024) * UINT64CONST(1024);
}

int
fb_summary_service_meta_low_watermark_percent(void)
{
	return fb_summary_service_meta_low_watermark_percent_setting;
}

/*
 * fb_show_progress_enabled
 *    GUC entry point.
 */

bool
fb_show_progress_enabled(void)
{
	return fb_show_progress;
}

static bool
fb_is_blank_string(const char *value)
{
	return value == NULL || value[0] == '\0';
}

static bool
fb_parse_spill_mode(const char *value,
					FbSpillMode *mode_out,
					const char **detail_out,
					const char **canonical_out)
{
	if (fb_is_blank_string(value))
	{
		*detail_out = FB_SPILL_MODE_GUC_NAME " cannot be empty.";
		return false;
	}

	if (pg_strcasecmp(value, "auto") == 0)
	{
		*mode_out = FB_SPILL_MODE_AUTO;
		*canonical_out = "auto";
		return true;
	}
	if (pg_strcasecmp(value, "memory") == 0)
	{
		*mode_out = FB_SPILL_MODE_MEMORY;
		*canonical_out = "memory";
		return true;
	}
	if (pg_strcasecmp(value, "disk") == 0)
	{
		*mode_out = FB_SPILL_MODE_DISK;
		*canonical_out = "disk";
		return true;
	}

	*detail_out = FB_SPILL_MODE_GUC_NAME " must be one of: auto, memory, disk.";
	return false;
}

static bool
fb_memory_limit_unit_multiplier(const char *unit,
								uint64 *multiplier_kb,
								bool *unit_is_bytes)
{
	if (unit == NULL || unit[0] == '\0' || pg_strcasecmp(unit, "kb") == 0)
	{
		*multiplier_kb = UINT64CONST(1);
		*unit_is_bytes = false;
		return true;
	}
	if (pg_strcasecmp(unit, "mb") == 0)
	{
		*multiplier_kb = UINT64CONST(1024);
		*unit_is_bytes = false;
		return true;
	}
	if (pg_strcasecmp(unit, "gb") == 0)
	{
		*multiplier_kb = UINT64CONST(1024) * UINT64CONST(1024);
		*unit_is_bytes = false;
		return true;
	}
	if (pg_strcasecmp(unit, "tb") == 0)
	{
		*multiplier_kb = UINT64CONST(1024) * UINT64CONST(1024) * UINT64CONST(1024);
		*unit_is_bytes = false;
		return true;
	}
	if (pg_strcasecmp(unit, "b") == 0)
	{
		*multiplier_kb = UINT64CONST(1);
		*unit_is_bytes = true;
		return true;
	}

	return false;
}

static bool
fb_memory_limit_parse_kb(const char *value,
						 uint64 *kb_out,
						 const char **detail_out)
{
	const char *ptr = value;
	char	   *endptr = NULL;
	unsigned long long raw_value;
	uint64		value_kb;
	uint64		multiplier_kb = UINT64CONST(1);
	bool		unit_is_bytes = false;
	char		unitbuf[3];
	int			unitlen = 0;

	while (*ptr != '\0' && isspace((unsigned char) *ptr))
		ptr++;

	if (*ptr == '\0')
	{
		*detail_out = FB_MEMORY_LIMIT_GUC_NAME " cannot be empty.";
		return false;
	}
	if (!isdigit((unsigned char) *ptr))
	{
		*detail_out = FB_MEMORY_LIMIT_GUC_NAME " must start with a positive integer.";
		return false;
	}

	errno = 0;
	raw_value = strtoull(ptr, &endptr, 10);
	if (endptr == ptr || errno == ERANGE)
	{
		*detail_out = FB_MEMORY_LIMIT_GUC_NAME " must start with a positive integer.";
		return false;
	}

	while (*endptr != '\0' && isspace((unsigned char) *endptr))
		endptr++;

	while (*endptr != '\0' && isalpha((unsigned char) *endptr))
	{
		if (unitlen >= (int) (sizeof(unitbuf) - 1))
		{
			*detail_out = FB_MEMORY_LIMIT_GUC_NAME " only accepts B, kB, MB, GB, or TB units.";
			return false;
		}

		unitbuf[unitlen++] = *endptr++;
	}
	unitbuf[unitlen] = '\0';

	while (*endptr != '\0' && isspace((unsigned char) *endptr))
		endptr++;

	if (*endptr != '\0')
	{
		*detail_out = FB_MEMORY_LIMIT_GUC_NAME " only accepts a number optionally followed by B, kB, MB, GB, or TB.";
		return false;
	}

	if (!fb_memory_limit_unit_multiplier(unitbuf, &multiplier_kb, &unit_is_bytes))
	{
		*detail_out = FB_MEMORY_LIMIT_GUC_NAME " only accepts B, kB, MB, GB, or TB units.";
		return false;
	}

	if (unit_is_bytes)
	{
		if ((raw_value % 1024ULL) != 0)
		{
			*detail_out = FB_MEMORY_LIMIT_GUC_NAME " byte values must be divisible by 1024.";
			return false;
		}
		value_kb = (uint64) raw_value / UINT64CONST(1024);
	}
	else
	{
		if ((uint64) raw_value > PG_UINT64_MAX / multiplier_kb)
		{
			*detail_out = FB_MEMORY_LIMIT_GUC_NAME " is too large.";
			return false;
		}
		value_kb = (uint64) raw_value * multiplier_kb;
	}

	*kb_out = value_kb;
	return true;
}

static char *
fb_memory_limit_canonical_text(uint64 kb)
{
	uint64 value = kb;
	const char *unit = "kB";
	char *text;
	char *result;

	if ((value % 1024) == 0)
	{
		value /= 1024;
		unit = "MB";
		if ((value % 1024) == 0)
		{
			value /= 1024;
			unit = "GB";
			if ((value % 1024) == 0)
			{
				value /= 1024;
				unit = "TB";
			}
		}
	}

	text = psprintf("%llu%s",
					(unsigned long long) value,
					unit);
	result = guc_strdup(ERROR, text);
	pfree(text);
	return result;
}

static bool
fb_spill_mode_check_hook(char **newval, void **extra, GucSource source)
{
	FbSpillMode mode;
	const char *detail = NULL;
	const char *canonical = NULL;
	FbSpillModeParseResult *result;

	(void) source;

	if (!fb_parse_spill_mode(*newval, &mode, &detail, &canonical))
	{
		if (detail != NULL)
			GUC_check_errdetail("%s", detail);
		return false;
	}

	guc_free(*newval);
	*newval = guc_strdup(ERROR, canonical);

	result = (FbSpillModeParseResult *) guc_malloc(ERROR, sizeof(*result));
	result->mode = mode;
	*extra = result;
	return true;
}

static void
fb_spill_mode_assign_hook(const char *newval, void *extra)
{
	FbSpillModeParseResult *result = (FbSpillModeParseResult *) extra;

	(void) newval;

	if (result != NULL)
		fb_spill_mode = result->mode;
}

static bool
fb_memory_limit_check_hook(char **newval, void **extra, GucSource source)
{
	char	   *canonical;
	const char *detail = NULL;
	uint64		parsed_kb;
	FbMemoryLimitParseResult *result;

	(void) source;

	if (fb_is_blank_string(*newval))
	{
		GUC_check_errdetail(FB_MEMORY_LIMIT_GUC_NAME " cannot be empty.");
		return false;
	}

	if (!fb_memory_limit_parse_kb(*newval, &parsed_kb, &detail))
	{
		if (detail != NULL)
			GUC_check_errdetail("%s", detail);
		return false;
	}

	if (parsed_kb < FB_MEMORY_LIMIT_MIN_KB || parsed_kb > FB_MEMORY_LIMIT_MAX_KB)
	{
		GUC_check_errdetail(FB_MEMORY_LIMIT_GUC_NAME " must be between 1kB and 32GB.");
		return false;
	}

	canonical = fb_memory_limit_canonical_text(parsed_kb);
	guc_free(*newval);
	*newval = canonical;

	result = (FbMemoryLimitParseResult *) guc_malloc(ERROR, sizeof(*result));
	result->kb = parsed_kb;
	*extra = result;
	return true;
}

static void
fb_memory_limit_assign_hook(const char *newval, void *extra)
{
	FbMemoryLimitParseResult *result = (FbMemoryLimitParseResult *) extra;

	(void) newval;

	if (result != NULL)
		fb_memory_limit_kb = result->kb;
}

static char *
fb_trim_trailing_slashes(const char *path)
{
	char *trimmed;
	int len;

	if (path == NULL)
		return NULL;

	trimmed = pstrdup(path);
	len = strlen(trimmed);
	while (len > 1 && trimmed[len - 1] == '/')
	{
		trimmed[len - 1] = '\0';
		len--;
	}

	return trimmed;
}

static const char *
fb_token_basename(const char *token)
{
	const char *slash;

	if (token == NULL)
		return "";

	slash = strrchr(token, '/');
	if (slash == NULL)
		return token;

	return slash + 1;
}

static bool
fb_token_matches_command(const char *token, const char *command_name)
{
	return strcmp(fb_token_basename(token), command_name) == 0;
}

static List *
fb_tokenize_shell_command(const char *command)
{
	List	   *tokens = NIL;
	StringInfoData buf;
	bool		in_single = false;
	bool		in_double = false;
	bool		escaped = false;
	const char *ptr;

	initStringInfo(&buf);

	for (ptr = command; ptr != NULL && *ptr != '\0'; ptr++)
	{
		char ch = *ptr;

		if (escaped)
		{
			appendStringInfoChar(&buf, ch);
			escaped = false;
			continue;
		}

		if (ch == '\\')
		{
			escaped = true;
			continue;
		}

		if (in_single)
		{
			if (ch == '\'')
				in_single = false;
			else
				appendStringInfoChar(&buf, ch);
			continue;
		}

		if (in_double)
		{
			if (ch == '"')
				in_double = false;
			else
				appendStringInfoChar(&buf, ch);
			continue;
		}

		if (ch == '\'')
		{
			in_single = true;
			continue;
		}

		if (ch == '"')
		{
			in_double = true;
			continue;
		}

		if (isspace((unsigned char) ch))
		{
			if (buf.len > 0)
			{
				tokens = lappend(tokens, pstrdup(buf.data));
				resetStringInfo(&buf);
			}
			continue;
		}

		if (ch == '&' && ptr[1] == '&')
		{
			if (buf.len > 0)
			{
				tokens = lappend(tokens, pstrdup(buf.data));
				resetStringInfo(&buf);
			}
			tokens = lappend(tokens, pstrdup("&&"));
			ptr++;
			continue;
		}

		if (ch == '|' || ch == ';' || ch == '<' || ch == '>' ||
			ch == '$' || ch == '`')
			return NIL;

		appendStringInfoChar(&buf, ch);
	}

	if (escaped || in_single || in_double)
		return NIL;

	if (buf.len > 0)
		tokens = lappend(tokens, pstrdup(buf.data));

	return tokens;
}

static char *
fb_archive_dir_from_percent_f_path(const char *token)
{
	size_t len;

	if (token == NULL || token[0] != '/')
		return NULL;

	len = strlen(token);
	if (len < 3)
		return NULL;

	if (!(token[len - 3] == '/' && token[len - 2] == '%' && token[len - 1] == 'f'))
		return NULL;

	if (len == 3)
		return pstrdup("/");

	return pnstrdup(token, len - 3);
}

static char *
fb_try_parse_cp_archive_dir(List *tokens)
{
	char *dest;

	if (list_length(tokens) == 3)
	{
		if (!fb_token_matches_command((char *) list_nth(tokens, 0), "cp"))
			return NULL;
		if (strcmp((char *) list_nth(tokens, 1), "%p") != 0)
			return NULL;

		return fb_archive_dir_from_percent_f_path((char *) list_nth(tokens, 2));
	}

	if (list_length(tokens) != 8)
		return NULL;

	if (!fb_token_matches_command((char *) list_nth(tokens, 0), "test"))
		return NULL;
	if (strcmp((char *) list_nth(tokens, 1), "!") != 0 ||
		strcmp((char *) list_nth(tokens, 2), "-f") != 0 ||
		strcmp((char *) list_nth(tokens, 4), "&&") != 0)
		return NULL;
	if (!fb_token_matches_command((char *) list_nth(tokens, 5), "cp"))
		return NULL;
	if (strcmp((char *) list_nth(tokens, 6), "%p") != 0)
		return NULL;

	dest = fb_archive_dir_from_percent_f_path((char *) list_nth(tokens, 3));
	if (dest == NULL)
		return NULL;

	if (strcmp((char *) list_nth(tokens, 3), (char *) list_nth(tokens, 7)) != 0)
	{
		pfree(dest);
		return NULL;
	}

	return dest;
}

static char *
fb_try_parse_pg_probackup_archive_dir(List *tokens)
{
	char	   *backup_dir = NULL;
	char	   *instance_name = NULL;
	char	   *trimmed_backup_dir;
	bool		wal_file_name_ok = false;
	int			i;

	if (list_length(tokens) < 2)
		return NULL;

	if (!fb_token_matches_command((char *) list_nth(tokens, 0), "pg_probackup"))
		return NULL;
	if (strcmp((char *) list_nth(tokens, 1), "archive-push") != 0)
		return NULL;

	for (i = 2; i < list_length(tokens); i++)
	{
		char *token = (char *) list_nth(tokens, i);

		if (strcmp(token, "-B") == 0)
		{
			if (i + 1 >= list_length(tokens))
				return NULL;
			backup_dir = (char *) list_nth(tokens, i + 1);
			i++;
			continue;
		}

		if (strncmp(token, "--instance=", 11) == 0)
		{
			instance_name = token + 11;
			continue;
		}

		if (strcmp(token, "--instance") == 0)
		{
			if (i + 1 >= list_length(tokens))
				return NULL;
			instance_name = (char *) list_nth(tokens, i + 1);
			i++;
			continue;
		}

		if (strncmp(token, "--wal-file-name=", 16) == 0)
		{
			wal_file_name_ok = strcmp(token + 16, "%f") == 0;
			continue;
		}

		if (strcmp(token, "--wal-file-name") == 0)
		{
			if (i + 1 >= list_length(tokens))
				return NULL;
			wal_file_name_ok = strcmp((char *) list_nth(tokens, i + 1), "%f") == 0;
			i++;
			continue;
		}

		if (strncmp(token, "--remote-", 9) == 0 ||
			strncmp(token, "--archive-", 10) == 0)
			return NULL;
	}

	if (backup_dir == NULL || instance_name == NULL || !wal_file_name_ok)
		return NULL;
	if (backup_dir[0] != '/')
		return NULL;

	trimmed_backup_dir = fb_trim_trailing_slashes(backup_dir);
	backup_dir = psprintf("%s/wal/%s", trimmed_backup_dir, instance_name);
	pfree(trimmed_backup_dir);
	return backup_dir;
}

static char *
fb_try_parse_archive_command_dir(const char *command)
{
	List *tokens;
	char *resolved;

	if (fb_is_blank_string(command))
		return NULL;

	tokens = fb_tokenize_shell_command(command);
	if (tokens == NIL)
		return NULL;

	resolved = fb_try_parse_cp_archive_dir(tokens);
	if (resolved != NULL)
	{
		list_free_deep(tokens);
		return resolved;
	}

	resolved = fb_try_parse_pg_probackup_archive_dir(tokens);
	list_free_deep(tokens);
	return resolved;
}

static char *
fb_get_core_config_string(const char *name)
{
	const char *varname = NULL;
	char *value;

	value = GetConfigOptionByName(name, &varname, true);
	if (value == NULL || value[0] == '\0')
		return NULL;

	return value;
}

static void
fb_raise_archive_autodiscovery_error(const char *detail)
{
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("could not determine WAL archive directory automatically"),
			 errdetail("%s", detail),
			 errhint("Set pg_flashback.archive_dest explicitly.")));
}

static char *
fb_resolve_archive_dir_internal(FbArchiveDirSource *source_out,
											 const char **setting_name_out,
											 bool error_if_missing)
{
	char *archive_library;
	char *archive_command;
	char *resolved;

	if (!fb_is_blank_string(fb_archive_dest))
	{
		if (source_out != NULL)
			*source_out = FB_ARCHIVE_DIR_SOURCE_EXPLICIT_DEST;
		if (setting_name_out != NULL)
			*setting_name_out = "pg_flashback.archive_dest";
		return pstrdup(fb_archive_dest);
	}

	if (!fb_is_blank_string(fb_archive_dir))
	{
		if (source_out != NULL)
			*source_out = FB_ARCHIVE_DIR_SOURCE_LEGACY_DIR;
		if (setting_name_out != NULL)
			*setting_name_out = "pg_flashback.archive_dir";
		return pstrdup(fb_archive_dir);
	}

	archive_library = fb_get_core_config_string("archive_library");
	if (!fb_is_blank_string(archive_library))
	{
		if (!error_if_missing)
		{
			if (archive_library != NULL)
				pfree(archive_library);
			return NULL;
		}

		fb_raise_archive_autodiscovery_error("PostgreSQL archive_library is set; pg_flashback does not infer a local archive directory from archive_library.");
	}
	if (archive_library != NULL)
		pfree(archive_library);

	archive_command = fb_get_core_config_string("archive_command");
	resolved = fb_try_parse_archive_command_dir(archive_command);
	if (resolved != NULL)
	{
		if (source_out != NULL)
			*source_out = FB_ARCHIVE_DIR_SOURCE_ARCHIVE_COMMAND;
		if (setting_name_out != NULL)
			*setting_name_out = "archive_command";
		if (archive_command != NULL)
			pfree(archive_command);
		return resolved;
	}

	if (!error_if_missing)
	{
		if (archive_command != NULL)
			pfree(archive_command);
		return NULL;
	}

	if (fb_is_blank_string(archive_command))
		fb_raise_archive_autodiscovery_error("Neither pg_flashback.archive_dest nor legacy pg_flashback.archive_dir is set, and PostgreSQL archive_command is empty.");

	fb_raise_archive_autodiscovery_error("PostgreSQL archive_command is set, but pg_flashback only auto-detects simple local copy commands and local pg_probackup archive-push commands.");
	return NULL;
}
