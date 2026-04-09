#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "summaryd/fb_summaryd_core.h"

static volatile sig_atomic_t summaryd_got_sigterm = false;

static void summaryd_print_usage(FILE *stream, const char *progname);
static bool summaryd_streq(const char *lhs, const char *rhs);
static bool summaryd_copy_field(char *dst, size_t dst_len, const char *src,
								const char *name);
static void summaryd_trim_inplace(char *value);
static bool summaryd_parse_bool_value(const char *value, bool *out);
static bool summaryd_find_config_path(int argc, char **argv,
									  char *path_out, size_t path_out_len);
static bool summaryd_load_config_file(const char *path, SummarydConfig *config);
static int summaryd_parse_args(int argc, char **argv, SummarydConfig *config);
static void summaryd_log(const char *fmt, ...);
static void summaryd_handle_signal(int sig);
static int summaryd_mkdir_p(const char *path);
static void summaryd_build_path(char *out, size_t out_len,
								const char *root, const char *suffix);
static void summaryd_format_timestamp_utc(char *out, size_t out_len);
static void summaryd_format_state_timestamp(long long ts, char *out, size_t out_len);
static bool summaryd_write_file_atomic(const char *path, const char *contents);
static bool summaryd_prepare_runtime_dirs(const SummarydConfig *config);
static bool summaryd_maybe_daemonize(const SummarydConfig *config);
static int summaryd_acquire_lockfile(const SummarydConfig *config);
static bool summaryd_publish_state_files(const SummarydConfig *config,
										 const SummarydState *state);
static bool summaryd_run_iteration(const SummarydConfig *config,
								   SummarydState *state);

static void
summaryd_print_usage(FILE *stream, const char *progname)
{
	fprintf(stream,
			"Usage: %s --pgdata PATH --archive-dest PATH [options]\n"
			"\n"
			"Options:\n"
			"  --pgdata PATH          PostgreSQL data directory\n"
			"  --archive-dest PATH    WAL archive directory used for summary build\n"
			"  --config PATH          Optional daemon config file (key=value)\n"
			"  --interval-ms N        Sweep interval in milliseconds (default: 1000)\n"
			"  --once                 Run one sweep and exit\n"
			"  --foreground           Stay in foreground instead of daemonizing\n"
			"  --help                 Show this help message\n"
			"  --version              Show version and exit\n",
			progname);
}

static bool
summaryd_streq(const char *lhs, const char *rhs)
{
	return lhs != NULL && rhs != NULL && strcmp(lhs, rhs) == 0;
}

static bool
summaryd_copy_field(char *dst, size_t dst_len, const char *src, const char *name)
{
	if (dst == NULL || dst_len == 0 || src == NULL || src[0] == '\0')
		return false;
	if (snprintf(dst, dst_len, "%s", src) >= (int) dst_len)
	{
		fprintf(stderr, "%s is too long\n", name);
		return false;
	}
	return true;
}

static void
summaryd_trim_inplace(char *value)
{
	char   *start = value;
	char   *end;

	if (value == NULL)
		return;

	while (*start != '\0' && isspace((unsigned char) *start))
		start++;
	if (start != value)
		memmove(value, start, strlen(start) + 1);

	end = value + strlen(value);
	while (end > value && isspace((unsigned char) end[-1]))
		end--;
	*end = '\0';
}

static bool
summaryd_parse_bool_value(const char *value, bool *out)
{
	if (summaryd_streq(value, "true") || summaryd_streq(value, "on") ||
		summaryd_streq(value, "yes") || summaryd_streq(value, "1"))
	{
		*out = true;
		return true;
	}
	if (summaryd_streq(value, "false") || summaryd_streq(value, "off") ||
		summaryd_streq(value, "no") || summaryd_streq(value, "0"))
	{
		*out = false;
		return true;
	}
	return false;
}

static bool
summaryd_find_config_path(int argc, char **argv, char *path_out, size_t path_out_len)
{
	int i;

	if (path_out != NULL && path_out_len > 0)
		path_out[0] = '\0';

	for (i = 1; i < argc; i++)
	{
		if (!summaryd_streq(argv[i], "--config"))
			continue;
		if (i + 1 >= argc)
		{
			fprintf(stderr, "missing value for --config\n");
			return false;
		}
		return summaryd_copy_field(path_out, path_out_len, argv[i + 1], "config path");
	}

	return true;
}

static bool
summaryd_load_config_file(const char *path, SummarydConfig *config)
{
	FILE   *file;
	char	line[SUMMARYD_CONNINFO_MAX];
	int		lineno = 0;

	if (path == NULL || path[0] == '\0')
		return true;

	file = fopen(path, "r");
	if (file == NULL)
	{
		fprintf(stderr, "could not open config file %s: %s\n", path, strerror(errno));
		return false;
	}

	while (fgets(line, sizeof(line), file) != NULL)
	{
		char   *eq;
		char   *key;
		char   *value;
		bool	bool_value;

		lineno++;
		summaryd_trim_inplace(line);
		if (line[0] == '\0' || line[0] == '#')
			continue;

		eq = strchr(line, '=');
		if (eq == NULL)
		{
			fprintf(stderr, "invalid config line %d in %s\n", lineno, path);
			fclose(file);
			return false;
		}

		*eq = '\0';
		key = line;
		value = eq + 1;
		summaryd_trim_inplace(key);
		summaryd_trim_inplace(value);

		if (summaryd_streq(key, "pgdata"))
		{
			if (!summaryd_copy_field(config->pgdata, sizeof(config->pgdata), value, "pgdata"))
			{
				fclose(file);
				return false;
			}
		}
		else if (summaryd_streq(key, "archive_dest"))
		{
			if (!summaryd_copy_field(config->archive_dest, sizeof(config->archive_dest),
								 value, "archive_dest"))
			{
				fclose(file);
				return false;
			}
		}
		else if (summaryd_streq(key, "interval_ms"))
		{
			config->interval_ms = atoi(value);
			if (config->interval_ms <= 0)
			{
				fprintf(stderr, "invalid interval_ms in %s:%d\n", path, lineno);
				fclose(file);
				return false;
			}
		}
		else if (summaryd_streq(key, "foreground"))
		{
			if (!summaryd_parse_bool_value(value, &bool_value))
			{
				fprintf(stderr, "invalid foreground value in %s:%d\n", path, lineno);
				fclose(file);
				return false;
			}
			config->foreground = bool_value;
		}
		else
		{
			fprintf(stderr, "unknown config key %s in %s:%d\n", key, path, lineno);
			fclose(file);
			return false;
		}
	}

	fclose(file);
	return true;
}

static int
summaryd_parse_args(int argc, char **argv, SummarydConfig *config)
{
	int i;
	char config_path[SUMMARYD_PATH_MAX];

	if (!summaryd_find_config_path(argc, argv, config_path, sizeof(config_path)))
		return -1;
	if (config_path[0] != '\0')
	{
		if (!summaryd_copy_field(config->config_path, sizeof(config->config_path),
								config_path, "config path"))
			return -1;
		if (!summaryd_load_config_file(config_path, config))
			return -1;
	}

	for (i = 1; i < argc; i++)
	{
		const char *arg = argv[i];

		if (summaryd_streq(arg, "--help"))
		{
			summaryd_print_usage(stdout, argv[0]);
			return 1;
		}
		if (summaryd_streq(arg, "--version"))
		{
			printf("pg_flashback-summaryd %s\n", PG_FLASHBACK_VERSION);
			return 1;
		}
		if (summaryd_streq(arg, "--foreground"))
		{
			config->foreground = true;
			continue;
		}
		if (summaryd_streq(arg, "--once"))
		{
			config->once = true;
			continue;
		}
		if (summaryd_streq(arg, "--pgdata") ||
			summaryd_streq(arg, "--archive-dest") ||
			summaryd_streq(arg, "--config") ||
			summaryd_streq(arg, "--interval-ms"))
		{
			const char *value;

			if (i + 1 >= argc)
			{
				fprintf(stderr, "missing value for %s\n", arg);
				return -1;
			}

			value = argv[++i];
			if (summaryd_streq(arg, "--pgdata"))
			{
				if (!summaryd_copy_field(config->pgdata, sizeof(config->pgdata), value,
									 "pgdata"))
					return -1;
			}
			else if (summaryd_streq(arg, "--archive-dest"))
			{
				if (!summaryd_copy_field(config->archive_dest,
									 sizeof(config->archive_dest), value,
									 "archive-dest"))
					return -1;
			}
			else if (summaryd_streq(arg, "--config"))
			{
				if (!summaryd_copy_field(config->config_path,
									 sizeof(config->config_path), value,
									 "config path"))
					return -1;
			}
			else
			{
				config->interval_ms = atoi(value);
				if (config->interval_ms <= 0)
				{
					fprintf(stderr, "invalid --interval-ms: %s\n", value);
					return -1;
				}
			}
			continue;
		}

		fprintf(stderr, "unknown option: %s\n", arg);
		return -1;
	}

	return 0;
}

static void
summaryd_log(const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	fputc('\n', stderr);
	va_end(args);
}

static void
summaryd_handle_signal(int sig)
{
	(void) sig;
	summaryd_got_sigterm = true;
}

static int
summaryd_mkdir_p(const char *path)
{
	char mutable[SUMMARYD_PATH_MAX];
	char *slash;

	if (path == NULL || path[0] == '\0')
		return -1;
	if (strlen(path) >= sizeof(mutable))
		return -1;

	strcpy(mutable, path);
	for (slash = mutable + 1; *slash != '\0'; slash++)
	{
		if (*slash != '/')
			continue;
		*slash = '\0';
		if (mkdir(mutable, 0700) != 0 && errno != EEXIST)
			return -1;
		*slash = '/';
	}
	if (mkdir(mutable, 0700) != 0 && errno != EEXIST)
		return -1;
	return 0;
}

static bool
summaryd_prepare_runtime_dirs(const SummarydConfig *config)
{
	char meta_dir[SUMMARYD_PATH_MAX];
	char summary_dir[SUMMARYD_PATH_MAX];
	char summaryd_dir[SUMMARYD_PATH_MAX];
	char recovered_dir[SUMMARYD_PATH_MAX];
	char runtime_dir[SUMMARYD_PATH_MAX];
	char hint_dir[SUMMARYD_PATH_MAX];

	summaryd_build_path(meta_dir, sizeof(meta_dir), config->pgdata, "pg_flashback/meta");
	summaryd_build_path(summary_dir, sizeof(summary_dir), config->pgdata,
						"pg_flashback/meta/summary");
	summaryd_build_path(summaryd_dir, sizeof(summaryd_dir), config->pgdata,
						"pg_flashback/meta/summaryd");
	summaryd_build_path(recovered_dir, sizeof(recovered_dir), config->pgdata,
						"pg_flashback/recovered_wal");
	summaryd_build_path(runtime_dir, sizeof(runtime_dir), config->pgdata, "pg_flashback/runtime");
	summaryd_build_path(hint_dir, sizeof(hint_dir), config->pgdata,
						"pg_flashback/runtime/summary-hints");

	return summaryd_mkdir_p(meta_dir) == 0 &&
		summaryd_mkdir_p(summary_dir) == 0 &&
		summaryd_mkdir_p(summaryd_dir) == 0 &&
		summaryd_mkdir_p(recovered_dir) == 0 &&
		summaryd_mkdir_p(runtime_dir) == 0 &&
		summaryd_mkdir_p(hint_dir) == 0;
}

static bool
summaryd_maybe_daemonize(const SummarydConfig *config)
{
	pid_t pid;
	int devnull;

	if (config->foreground || config->once)
		return true;

	pid = fork();
	if (pid < 0)
		return false;
	if (pid > 0)
		exit(0);

	if (setsid() < 0)
		return false;

	devnull = open("/dev/null", O_RDWR);
	if (devnull < 0)
		return false;

	(void) dup2(devnull, STDIN_FILENO);
	(void) dup2(devnull, STDOUT_FILENO);
	(void) dup2(devnull, STDERR_FILENO);
	if (devnull > STDERR_FILENO)
		close(devnull);
	(void) chdir("/");
	return true;
}

static int
summaryd_acquire_lockfile(const SummarydConfig *config)
{
	char lock_path[SUMMARYD_PATH_MAX];
	struct flock lock;
	int fd;

	summaryd_build_path(lock_path, sizeof(lock_path), config->pgdata,
						"pg_flashback/meta/summaryd/lock");
	fd = open(lock_path, O_RDWR | O_CREAT, 0600);
	if (fd < 0)
		return -1;

	memset(&lock, 0, sizeof(lock));
	lock.l_type = F_WRLCK;
	lock.l_whence = SEEK_SET;
	if (fcntl(fd, F_SETLK, &lock) != 0)
	{
		close(fd);
		return -1;
	}

	if (ftruncate(fd, 0) == 0)
	{
		char pidbuf[64];
		int written = snprintf(pidbuf, sizeof(pidbuf), "%ld\n", (long) getpid());

		if (written > 0)
		{
			(void) write(fd, pidbuf, (size_t) written);
			(void) fsync(fd);
		}
	}

	return fd;
}

static void
summaryd_build_path(char *out, size_t out_len, const char *root, const char *suffix)
{
	if (snprintf(out, out_len, "%s/%s", root, suffix) >= (int) out_len)
	{
		fprintf(stderr, "path too long for %s/%s\n", root, suffix);
		exit(2);
	}
}

static void
summaryd_format_timestamp_utc(char *out, size_t out_len)
{
	time_t now = time(NULL);
	struct tm tm_utc;

	gmtime_r(&now, &tm_utc);
	strftime(out, out_len, "%Y-%m-%d %H:%M:%S+00", &tm_utc);
}

static bool
summaryd_write_file_atomic(const char *path, const char *contents)
{
	char tmp_path[SUMMARYD_PATH_MAX];
	FILE *file;

	if (snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.%d", path, getpid()) >=
		(int) sizeof(tmp_path))
		return false;

	file = fopen(tmp_path, "wb");
	if (file == NULL)
		return false;
	if (contents != NULL && contents[0] != '\0')
	{
		size_t len = strlen(contents);

		if (fwrite(contents, 1, len, file) != len)
		{
			fclose(file);
			unlink(tmp_path);
			return false;
		}
	}
	if (fflush(file) != 0 || fsync(fileno(file)) != 0)
	{
		fclose(file);
		unlink(tmp_path);
		return false;
	}
	if (fclose(file) != 0)
	{
		unlink(tmp_path);
		return false;
	}
	if (rename(tmp_path, path) != 0)
	{
		unlink(tmp_path);
		return false;
	}
	return true;
}

static bool
summaryd_publish_state_files(const SummarydConfig *config, const SummarydState *state)
{
	char summaryd_dir[SUMMARYD_PATH_MAX];
	char state_path[SUMMARYD_PATH_MAX];
	char debug_path[SUMMARYD_PATH_MAX];
	char state_json[4096];
	char debug_json[4096];
	char last_scan_at[64];
	char throughput_window_started_at[64];
	char last_build_at[64];

	summaryd_build_path(summaryd_dir, sizeof(summaryd_dir), config->pgdata,
						"pg_flashback/meta/summaryd");
	summaryd_build_path(state_path, sizeof(state_path), summaryd_dir, "state.json");
	summaryd_build_path(debug_path, sizeof(debug_path), summaryd_dir, "debug.json");

	if (summaryd_mkdir_p(summaryd_dir) != 0)
		return false;

	summaryd_format_state_timestamp(state->last_scan_at_internal,
									 last_scan_at,
									 sizeof(last_scan_at));
	summaryd_format_state_timestamp(state->throughput_window_started_at_internal,
									 throughput_window_started_at,
									 sizeof(throughput_window_started_at));
	summaryd_format_state_timestamp(state->last_build_at_internal,
									 last_build_at,
									 sizeof(last_build_at));

	snprintf(state_json, sizeof(state_json),
			 "{\"service_enabled\":%s,\"daemon_pid\":%d,"
			 "\"registered_workers\":%d,\"active_workers\":%d,"
			 "\"queue_capacity\":%d,\"hot_window\":%d,"
			 "\"pending_hot\":%d,\"pending_cold\":%d,"
			 "\"running_hot\":%d,\"running_cold\":%d,"
			 "\"snapshot_timeline_id\":%u,"
			 "\"snapshot_oldest_segno\":%llu,"
			 "\"snapshot_newest_segno\":%llu,"
			 "\"snapshot_hot_candidates\":%d,"
			 "\"snapshot_cold_candidates\":%d,"
			 "\"scan_count\":%llu,\"enqueue_count\":%llu,"
			 "\"build_count\":%llu,\"cleanup_count\":%llu,"
			 "\"last_scan_at\":\"%s\","
			 "\"throughput_window_started_at\":\"%s\","
			 "\"last_build_at\":\"%s\","
			 "\"throughput_window_builds\":%llu}\n",
			 state->service_enabled ? "true" : "false",
			 state->daemon_pid,
			 state->registered_workers,
			 state->active_workers,
			 state->queue_capacity,
			 state->hot_window,
			 state->pending_hot,
			 state->pending_cold,
			 state->running_hot,
			 state->running_cold,
			 state->snapshot_timeline_id,
			 state->snapshot_oldest_segno,
			 state->snapshot_newest_segno,
			 state->snapshot_hot_candidates,
			 state->snapshot_cold_candidates,
			 state->scan_count,
			 state->enqueue_count,
			 state->build_count,
			 state->cleanup_count,
			 last_scan_at,
			 throughput_window_started_at,
			 last_build_at,
			 state->throughput_window_builds);

	snprintf(debug_json, sizeof(debug_json),
			 "{\"service_enabled\":%s,\"daemon_pid\":%d,"
			 "\"registered_workers\":%d,\"active_workers\":%d,"
			 "\"queue_capacity\":%d,\"hot_window\":%d,"
			 "\"pending_hot\":%d,\"pending_cold\":%d,"
			 "\"running_hot\":%d,\"running_cold\":%d,"
			 "\"snapshot_timeline_id\":%u,"
			 "\"snapshot_oldest_segno\":%llu,"
			 "\"snapshot_newest_segno\":%llu,"
			 "\"snapshot_hot_candidates\":%d,"
			 "\"snapshot_cold_candidates\":%d,"
			 "\"scan_count\":%llu,\"enqueue_count\":%llu,"
			 "\"build_count\":%llu,\"cleanup_count\":%llu,"
			 "\"last_scan_at\":\"%s\","
			 "\"throughput_window_started_at\":\"%s\","
			 "\"last_build_at\":\"%s\","
			 "\"throughput_window_builds\":%llu,"
			 "\"last_error\":\"%s\","
			 "\"pgdata\":\"%s\",\"archive_dest\":\"%s\",\"interval_ms\":%d}\n",
			 state->service_enabled ? "true" : "false",
			 state->daemon_pid,
			 state->registered_workers,
			 state->active_workers,
			 state->queue_capacity,
			 state->hot_window,
			 state->pending_hot,
			 state->pending_cold,
			 state->running_hot,
			 state->running_cold,
			 state->snapshot_timeline_id,
			 state->snapshot_oldest_segno,
			 state->snapshot_newest_segno,
			 state->snapshot_hot_candidates,
			 state->snapshot_cold_candidates,
			 state->scan_count,
			 state->enqueue_count,
			 state->build_count,
			 state->cleanup_count,
			 last_scan_at,
			 throughput_window_started_at,
			 last_build_at,
			 state->throughput_window_builds,
			 state->last_error,
			 config->pgdata,
			 config->archive_dest,
			 config->interval_ms);

	return summaryd_write_file_atomic(state_path, state_json) &&
		summaryd_write_file_atomic(debug_path, debug_json);
}

static void
summaryd_format_state_timestamp(long long ts, char *out, size_t out_len)
{
	time_t unix_sec;
	struct tm tm_utc;

	if (out == NULL || out_len == 0)
		return;
	if (ts == 0)
	{
		out[0] = '\0';
		return;
	}

	unix_sec = (time_t) ((ts / 1000000LL) + 946684800LL);
	gmtime_r(&unix_sec, &tm_utc);
	strftime(out, out_len, "%Y-%m-%d %H:%M:%S+00", &tm_utc);
}

static bool
summaryd_run_iteration(const SummarydConfig *config, SummarydState *state)
{
	return summaryd_run_core_iteration(config, state, summaryd_publish_state_files);
}

int
main(int argc, char **argv)
{
	SummarydConfig config;
	SummarydState state;
	int parse_result;

	memset(&config, 0, sizeof(config));
	memset(&state, 0, sizeof(state));
	config.interval_ms = 1000;

	parse_result = summaryd_parse_args(argc, argv, &config);
	if (parse_result > 0)
		return 0;
	if (parse_result < 0)
	{
		summaryd_print_usage(stderr, argv[0]);
		return 2;
	}

	if (config.pgdata[0] == '\0' || config.archive_dest[0] == '\0')
	{
		fprintf(stderr, "both --pgdata and --archive-dest are required\n");
		summaryd_print_usage(stderr, argv[0]);
		return 2;
	}

	if (!summaryd_prepare_runtime_dirs(&config))
	{
		summaryd_log("failed to prepare runtime directories under %s", config.pgdata);
		return 1;
	}

	signal(SIGINT, summaryd_handle_signal);
	signal(SIGTERM, summaryd_handle_signal);

	if (!summaryd_maybe_daemonize(&config))
	{
		summaryd_log("failed to daemonize");
		return 1;
	}

	{
		int lock_fd = summaryd_acquire_lockfile(&config);

		if (lock_fd < 0)
		{
			summaryd_log("another pg_flashback-summaryd already holds the cluster lock");
			return 1;
		}

		for (;;)
		{
			bool ok = summaryd_run_iteration(&config, &state);
			struct timespec req;

			if (!summaryd_publish_state_files(&config, &state))
			{
				summaryd_log("failed to publish state files under %s", config.pgdata);
				if (config.once)
				{
					close(lock_fd);
					return 1;
				}
			}

			if (config.once)
			{
				close(lock_fd);
				return ok ? 0 : 1;
			}
			if (summaryd_got_sigterm)
				break;

			req.tv_sec = config.interval_ms / 1000;
			req.tv_nsec = (long) (config.interval_ms % 1000) * 1000000L;
			nanosleep(&req, NULL);
		}

		close(lock_fd);
	}

	return 0;
}
