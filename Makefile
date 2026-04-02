EXTENSION = pg_flashback
MODULE_big = pg_flashback
OBJS = \
	src/fb_entry.o \
	src/fb_custom_scan.o \
	src/fb_runtime.o \
	src/fb_ckwal.o \
	src/fb_compat.o \
	src/fb_guc.o \
	src/fb_progress.o \
	src/fb_catalog.o \
	src/fb_error.o \
	src/fb_summary.o \
	src/fb_summary_service.o \
	src/fb_spool.o \
	src/fb_apply.o \
	src/fb_wal.o \
	src/fb_replay.o \
	src/fb_reverse_ops.o \
	src/fb_apply_keyed.o \
	src/fb_apply_bag.o \
	src/fb_toast.o

DATA = sql/pg_flashback--0.1.0.sql
REGRESS = fb_smoke fb_relation_gate fb_relation_unsupported fb_runtime_gate fb_flashback_keyed fb_flashback_bag fb_flashback_storage_boundary fb_flashback_hot_update_fpw fb_flashback_main_truncate fb_flashback_standby_lock fb_flashback_toast_storage_boundary fb_guc_defaults pg_flashback fb_user_surface fb_recordref fb_replay fb_wal_sidecar fb_wal_parallel_payload fb_apply_parallel fb_wal_source_policy fb_recovered_wal_policy fb_wal_prefix_suffix fb_wal_error_surface fb_memory_limit fb_spill fb_preflight fb_toast_flashback fb_progress fb_value_per_call fb_custom_scan fb_flashback_full_output fb_summary_prefilter fb_summary_service fb_runtime_cleanup fb_summary_v3 fb_summary_overlap_toast

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

override CPPFLAGS += -I$(CURDIR)/include
PG_CFLAGS += -pthread
SHLIB_LINK += -pthread

ifdef USE_PGXS
include $(PGXS)
else
include $(PGXS)
endif
