/*
 * fb_summary_state.c
 *    State-file helpers for external summary daemon integration.
 */

#include "postgres.h"

#include <ctype.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

#include "fb_runtime.h"
#include "fb_summary_state.h"

#define FB_SUMMARY_DAEMON_STALE_SECS 10

static char *fb_summary_state_read_file(const char *path);
static bool fb_summary_state_find_value(const char *json,
										const char *key,
										const char **value_out);
static bool fb_summary_state_parse_bool(const char *json,
										 const char *key,
										 bool *out);
static bool fb_summary_state_parse_int(const char *json,
										const char *key,
										int *out);
static bool fb_summary_state_parse_uint64(const char *json,
										   const char *key,
										   uint64 *out);
static bool fb_summary_state_parse_timestamp(const char *json,
											  const char *key,
											  TimestampTz *out);
static void fb_summary_state_write_file_atomic(const char *path,
												 const char *contents);

static char *
fb_summary_state_read_file(const char *path)
{
	FILE   *file;
	long	size;
	char   *buffer;

	file = AllocateFile(path, "rb");
	if (file == NULL)
		return NULL;
	if (fseek(file, 0, SEEK_END) != 0)
	{
		FreeFile(file);
		return NULL;
	}
	size = ftell(file);
	if (size < 0)
	{
		FreeFile(file);
		return NULL;
	}
	if (fseek(file, 0, SEEK_SET) != 0)
	{
		FreeFile(file);
		return NULL;
	}
	buffer = palloc((Size) size + 1);
	if (size > 0 && fread(buffer, 1, (Size) size, file) != (Size) size)
	{
		pfree(buffer);
		FreeFile(file);
		return NULL;
	}
	buffer[size] = '\0';
	FreeFile(file);
	return buffer;
}

static bool
fb_summary_state_find_value(const char *json,
							const char *key,
							const char **value_out)
{
	char   *pattern;
	const char *pos;

	if (value_out != NULL)
		*value_out = NULL;
	if (json == NULL || key == NULL)
		return false;

	pattern = psprintf("\"%s\"", key);
	pos = strstr(json, pattern);
	pfree(pattern);
	if (pos == NULL)
		return false;
	pos = strchr(pos, ':');
	if (pos == NULL)
		return false;
	pos++;
	while (*pos != '\0' && isspace((unsigned char) *pos))
		pos++;
	if (*pos == '\0')
		return false;
	if (value_out != NULL)
		*value_out = pos;
	return true;
}

static bool
fb_summary_state_parse_bool(const char *json, const char *key, bool *out)
{
	const char *value;

	if (!fb_summary_state_find_value(json, key, &value))
		return false;
	if (strncmp(value, "true", 4) == 0)
	{
		if (out != NULL)
			*out = true;
		return true;
	}
	if (strncmp(value, "false", 5) == 0)
	{
		if (out != NULL)
			*out = false;
		return true;
	}
	return false;
}

static bool
fb_summary_state_parse_int(const char *json, const char *key, int *out)
{
	const char *value;
	char   *endptr = NULL;
	long	result;

	if (!fb_summary_state_find_value(json, key, &value))
		return false;
	errno = 0;
	result = strtol(value, &endptr, 10);
	if (errno != 0 || endptr == value)
		return false;
	if (out != NULL)
		*out = (int) result;
	return true;
}

static bool
fb_summary_state_parse_uint64(const char *json, const char *key, uint64 *out)
{
	const char *value;
	char   *endptr = NULL;
	unsigned long long result;

	if (!fb_summary_state_find_value(json, key, &value))
		return false;
	errno = 0;
	result = strtoull(value, &endptr, 10);
	if (errno != 0 || endptr == value)
		return false;
	if (out != NULL)
		*out = (uint64) result;
	return true;
}

static bool
fb_summary_state_parse_timestamp(const char *json,
								 const char *key,
								 TimestampTz *out)
{
	const char *value;
	const char *endquote;
	char   *timestamp_text;

	if (!fb_summary_state_find_value(json, key, &value))
		return false;
	if (*value != '"')
		return false;
	value++;
	endquote = strchr(value, '"');
	if (endquote == NULL)
		return false;
	if (endquote == value)
	{
		if (out != NULL)
			*out = 0;
		return true;
	}
	timestamp_text = pnstrdup(value, endquote - value);
	if (out != NULL)
	{
		*out = DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
													   CStringGetDatum(timestamp_text),
													   ObjectIdGetDatum(InvalidOid),
													   Int32GetDatum(-1)));
	}
	pfree(timestamp_text);
	return true;
}

static void
fb_summary_state_write_file_atomic(const char *path, const char *contents)
{
	char   *tmp_path;
	FILE   *file;

	tmp_path = psprintf("%s.tmp.%d", path, MyProcPid);
	file = AllocateFile(tmp_path, "wb");
	if (file == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create summary state temp file"),
				 errdetail("path=%s: %m", tmp_path)));
	if (contents != NULL && contents[0] != '\0')
	{
		Size len = strlen(contents);

		if (fwrite(contents, 1, len, file) != len)
		{
			FreeFile(file);
			unlink(tmp_path);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write summary state temp file"),
					 errdetail("path=%s: %m", tmp_path)));
		}
	}
	if (fflush(file) != 0 || pg_fsync(fileno(file)) != 0)
	{
		FreeFile(file);
		unlink(tmp_path);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync summary state temp file"),
				 errdetail("path=%s: %m", tmp_path)));
	}
	if (FreeFile(file) != 0)
	{
		unlink(tmp_path);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close summary state temp file"),
				 errdetail("path=%s: %m", tmp_path)));
	}
	if (rename(tmp_path, path) != 0)
	{
		unlink(tmp_path);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not publish summary state file"),
				 errdetail("path=%s: %m", path)));
	}
	pfree(tmp_path);
}

static bool
fb_summary_state_load_common(const char *path, FbSummaryDaemonState *state)
{
	char   *json;
	struct stat st;
	time_t	now_sec = time(NULL);

	if (state != NULL)
		MemSet(state, 0, sizeof(*state));

	if (stat(path, &st) != 0)
		return false;

	json = fb_summary_state_read_file(path);
	if (json == NULL)
		return false;

	if (state != NULL)
	{
		state->present = true;
		state->published_at = time_t_to_timestamptz((pg_time_t) st.st_mtime);
		(void) fb_summary_state_parse_bool(json, "service_enabled", &state->service_enabled);
		(void) fb_summary_state_parse_int(json, "daemon_pid", &state->daemon_pid);
		(void) fb_summary_state_parse_int(json, "registered_workers", &state->registered_workers);
		(void) fb_summary_state_parse_int(json, "active_workers", &state->active_workers);
		(void) fb_summary_state_parse_int(json, "queue_capacity", &state->queue_capacity);
		(void) fb_summary_state_parse_int(json, "hot_window", &state->hot_window);
		(void) fb_summary_state_parse_int(json, "pending_hot", &state->pending_hot);
		(void) fb_summary_state_parse_int(json, "pending_cold", &state->pending_cold);
		(void) fb_summary_state_parse_int(json, "running_hot", &state->running_hot);
		(void) fb_summary_state_parse_int(json, "running_cold", &state->running_cold);
		(void) fb_summary_state_parse_int(json, "snapshot_timeline_id",
										  (int *) &state->snapshot_timeline_id);
		(void) fb_summary_state_parse_uint64(json, "snapshot_oldest_segno",
											 (uint64 *) &state->snapshot_oldest_segno);
		(void) fb_summary_state_parse_uint64(json, "snapshot_newest_segno",
											 (uint64 *) &state->snapshot_newest_segno);
		(void) fb_summary_state_parse_int(json, "snapshot_hot_candidates",
										  &state->snapshot_hot_candidates);
		(void) fb_summary_state_parse_int(json, "snapshot_cold_candidates",
										  &state->snapshot_cold_candidates);
		(void) fb_summary_state_parse_uint64(json, "scan_count", &state->scan_count);
		(void) fb_summary_state_parse_uint64(json, "enqueue_count", &state->enqueue_count);
		(void) fb_summary_state_parse_uint64(json, "build_count", &state->build_count);
		(void) fb_summary_state_parse_uint64(json, "cleanup_count", &state->cleanup_count);
		(void) fb_summary_state_parse_timestamp(json, "last_scan_at", &state->last_scan_at);
		(void) fb_summary_state_parse_timestamp(json, "throughput_window_started_at",
											  &state->throughput_window_started_at);
		(void) fb_summary_state_parse_timestamp(json, "last_build_at",
											  &state->last_build_at);
		(void) fb_summary_state_parse_uint64(json, "throughput_window_builds",
											 &state->throughput_window_builds);
		if ((now_sec - st.st_mtime) > FB_SUMMARY_DAEMON_STALE_SECS)
		{
			state->stale = true;
			state->service_enabled = false;
		}
	}

	pfree(json);
	return true;
}

bool
fb_summary_state_load(FbSummaryDaemonState *state)
{
	char *path = fb_runtime_summaryd_state_path();
	bool loaded = fb_summary_state_load_common(path, state);

	pfree(path);
	return loaded;
}

bool
fb_summary_debug_state_load(FbSummaryDaemonState *state)
{
	char *path = fb_runtime_summaryd_debug_path();
	bool loaded = fb_summary_state_load_common(path, state);

	pfree(path);
	return loaded;
}

bool
fb_summary_query_hint_load(FbSummaryQueryHint *hint)
{
	char   *path;
	char   *json;

	if (hint != NULL)
		MemSet(hint, 0, sizeof(*hint));

	path = fb_runtime_summary_last_query_hint_path();
	json = fb_summary_state_read_file(path);
	pfree(path);
	if (json == NULL)
		return false;

	if (hint != NULL)
	{
		int value = 0;

		hint->present = true;
		(void) fb_summary_state_parse_timestamp(json, "observed_at", &hint->observed_at);
		if (fb_summary_state_parse_int(json, "summary_span_fallback_segments", &value))
			hint->summary_span_fallback_segments = (uint32) Max(value, 0);
		value = 0;
		if (fb_summary_state_parse_int(json, "metadata_fallback_segments", &value))
			hint->metadata_fallback_segments = (uint32) Max(value, 0);
	}

	pfree(json);
	return true;
}

void
fb_summary_query_hint_write(TimestampTz observed_at,
							uint32 summary_span_fallback_segments,
							uint32 metadata_fallback_segments)
{
	char   *path;
	char   *json;
	char   *observed_at_text;

	fb_runtime_ensure_summary_daemon_dirs();
	path = fb_runtime_summary_last_query_hint_path();
	observed_at_text = DatumGetCString(DirectFunctionCall1(timestamptz_out,
														   TimestampTzGetDatum(observed_at)));
	json = psprintf("{\"observed_at\":\"%s\",\"summary_span_fallback_segments\":%u,"
					"\"metadata_fallback_segments\":%u}\n",
					observed_at_text,
					summary_span_fallback_segments,
					metadata_fallback_segments);
	fb_summary_state_write_file_atomic(path, json);
	pfree(observed_at_text);
	pfree(json);
	pfree(path);
}
