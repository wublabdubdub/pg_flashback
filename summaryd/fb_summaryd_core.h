#ifndef FB_SUMMARYD_CORE_H
#define FB_SUMMARYD_CORE_H

#include <stdbool.h>

#define SUMMARYD_PATH_MAX 4096
#define SUMMARYD_CONNINFO_MAX 2048

typedef struct SummarydConfig
{
	char pgdata[SUMMARYD_PATH_MAX];
	char archive_dest[SUMMARYD_PATH_MAX];
	char config_path[SUMMARYD_PATH_MAX];
	char conninfo[SUMMARYD_CONNINFO_MAX];
	int interval_ms;
	bool foreground;
	bool once;
	bool conninfo_present;
} SummarydConfig;

typedef struct SummarydState
{
	bool service_enabled;
	int daemon_pid;
	int registered_workers;
	int active_workers;
	int queue_capacity;
	int hot_window;
	int pending_hot;
	int pending_cold;
	int running_hot;
	int running_cold;
	unsigned int snapshot_timeline_id;
	unsigned long long snapshot_oldest_segno;
	unsigned long long snapshot_newest_segno;
	int snapshot_hot_candidates;
	int snapshot_cold_candidates;
	unsigned long long scan_count;
	unsigned long long enqueue_count;
	unsigned long long build_count;
	unsigned long long cleanup_count;
	unsigned long long throughput_window_builds;
	long long last_scan_at_internal;
	long long throughput_window_started_at_internal;
	long long last_build_at_internal;
	char last_error[512];
} SummarydState;

void summaryd_core_set_paths(const SummarydConfig *config);
const char *summaryd_core_last_error(void);
void summaryd_core_clear_error(void);
typedef bool (*SummarydStatePublishHook) (const SummarydConfig *config,
										  const SummarydState *state);
bool summaryd_run_core_iteration(const SummarydConfig *config,
								 SummarydState *state,
								 SummarydStatePublishHook publish_hook);

#endif
