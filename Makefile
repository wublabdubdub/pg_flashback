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
	src/fb_summary_state.o \
	src/fb_summary_service.o \
	src/fb_spool.o \
	src/fb_apply.o \
	src/fb_wal.o \
	src/fb_replay.o \
	src/fb_reverse_ops.o \
	src/fb_apply_keyed.o \
	src/fb_apply_bag.o \
	src/fb_toast.o

DATA = sql/pg_flashback--0.1.0.sql sql/pg_flashback--0.1.1.sql sql/pg_flashback--0.2.0.sql sql/pg_flashback--0.2.1.sql sql/pg_flashback--0.1.0--0.1.1.sql sql/pg_flashback--0.1.1--0.2.0.sql sql/pg_flashback--0.2.0--0.2.1.sql
REGRESS = fb_smoke fb_relation_gate fb_relation_unsupported fb_runtime_gate fb_flashback_keyed fb_flashback_bag fb_flashback_storage_boundary fb_flashback_hot_update_fpw fb_flashback_main_truncate fb_flashback_standby_lock fb_flashback_toast_storage_boundary fb_guc_defaults fb_guc_startup fb_target_snapshot pg_flashback fb_user_surface fb_extension_upgrade fb_recordref fb_replay fb_replay_prune_future_state fb_wal_sidecar fb_wal_parallel_payload fb_payload_scan_mode fb_summary_payload_locator_merge fb_apply_parallel fb_wal_source_policy fb_recovered_wal_policy fb_wal_prefix_suffix fb_wal_error_surface fb_memory_limit fb_spill fb_preflight fb_toast_flashback fb_progress fb_value_per_call fb_custom_scan fb_flashback_full_output fb_summary_prefilter fb_summary_service fb_summary_daemon_state fb_summary_identity fb_runtime_cleanup fb_summary_v3 fb_summary_overlap_toast

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
BINDIR := $(shell $(PG_CONFIG) --bindir)
SHAREDIR := $(shell $(PG_CONFIG) --sharedir)
INCLUDEDIR := $(shell $(PG_CONFIG) --includedir)
LIBDIR := $(shell $(PG_CONFIG) --libdir)
VERSION := $(shell cat VERSION)
SUMMARYD = pg_flashback-summaryd
SUMMARYD_BIN = summaryd/$(SUMMARYD)
SUMMARYD_SRCS = \
	summaryd/pg_flashback_summaryd.c \
	summaryd/fb_summaryd_core.c \
	summaryd/fb_summaryd_standalone_shim.c
SUMMARYD_OBJS = \
	summaryd/pg_flashback_summaryd.o \
	summaryd/fb_summaryd_core.o \
	summaryd/fb_summaryd_standalone_shim.o \
	summaryd/fb_summary_standalone.o \
	summaryd/vendor/xlogreader.o \
	summaryd/vendor/xactdesc.o \
	summaryd/vendor/dynahash.o
SUMMARYD_SAMPLE_CONF = summaryd/pg_flashback-summaryd.conf.sample

override CPPFLAGS += -I$(CURDIR)/include
PG_CFLAGS += -pthread
SHLIB_LINK += -pthread

ifdef USE_PGXS
include $(PGXS)
else
include $(PGXS)
endif

all: $(SUMMARYD_BIN)

summaryd/pg_flashback_summaryd.o: summaryd/pg_flashback_summaryd.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -Wall -Wextra -std=c11 -I$(INCLUDEDIR) -I$(shell $(PG_CONFIG) --includedir-server) -I$(CURDIR) -DPG_FLASHBACK_VERSION=\"$(VERSION)\" -c -o $@ $<

summaryd/fb_summaryd_core.o: summaryd/fb_summaryd_core.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -Wall -Wextra -std=c11 -I$(INCLUDEDIR) -I$(shell $(PG_CONFIG) --includedir-server) -I$(CURDIR) -c -o $@ $<

summaryd/fb_summaryd_standalone_shim.o: summaryd/fb_summaryd_standalone_shim.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -Wall -Wextra -std=c11 -I$(INCLUDEDIR) -I$(CURDIR) -I$(shell $(PG_CONFIG) --includedir-server) -c -o $@ $<

summaryd/fb_summary_standalone.o: src/fb_summary.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DPOSIX_FADV_SEQUENTIAL=2 -ffunction-sections -fdata-sections -Wall -Wextra -std=c11 -I$(INCLUDEDIR) -I$(shell $(PG_CONFIG) --includedir-server) -I$(CURDIR)/include -c -o $@ $<

summaryd/vendor/xlogreader.o: summaryd/vendor/xlogreader.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DFRONTEND -ffunction-sections -fdata-sections -Wall -Wextra -std=c11 -I$(INCLUDEDIR) -I$(shell $(PG_CONFIG) --includedir-server) -c -o $@ $<

summaryd/vendor/xactdesc.o: summaryd/vendor/xactdesc.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -Wall -Wextra -std=c11 -I$(INCLUDEDIR) -I$(shell $(PG_CONFIG) --includedir-server) -c -o $@ $<

summaryd/vendor/dynahash.o: summaryd/vendor/dynahash.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -Wall -Wextra -std=c11 -I$(INCLUDEDIR) -I$(shell $(PG_CONFIG) --includedir-server) -c -o $@ $<

$(SUMMARYD_BIN): $(SUMMARYD_OBJS)
	$(CC) $(CFLAGS) -Wall -Wextra -std=c11 -o $@ $(SUMMARYD_OBJS) $(LIBDIR)/libpgcommon.a $(LIBDIR)/libpgport.a -L$(LIBDIR) -Wl,-rpath,$(LIBDIR) -Wl,--gc-sections -lz

.PHONY: daemon install-daemon install-service check-summaryd

daemon: $(SUMMARYD_BIN)

install: install-daemon install-service

install-daemon: $(SUMMARYD_BIN)
	$(MKDIR_P) '$(DESTDIR)$(BINDIR)'
	$(INSTALL_PROGRAM) $(SUMMARYD_BIN) '$(DESTDIR)$(BINDIR)/$(SUMMARYD)'

install-service:
	$(MKDIR_P) '$(DESTDIR)$(SHAREDIR)/pg_flashback'
	$(INSTALL_DATA) $(SUMMARYD_SAMPLE_CONF) '$(DESTDIR)$(SHAREDIR)/pg_flashback/pg_flashback-summaryd.conf.sample'

check-summaryd: $(SUMMARYD_BIN)
	bash tests/summaryd/help_smoke.sh
	bash tests/summaryd/config_smoke.sh
	bash tests/summaryd/no_conninfo_smoke.sh
	bash tests/summaryd/summary_runner_smoke.sh
	bash tests/summaryd/readme_surface_smoke.sh
	bash tests/summaryd/bootstrap_nonroot_smoke.sh
	bash tests/summaryd/bootstrap_help_smoke.sh
	bash tests/summaryd/bootstrap_manual_runner_smoke.sh
	bash tests/summaryd/bootstrap_archive_autodiscovery_smoke.sh
	bash tests/summaryd/bootstrap_remove_legacy_service_smoke.sh
	bash tests/summaryd/bootstrap_prompt_defaults_smoke.sh
	bash tests/summaryd/bootstrap_prompt_validation_smoke.sh
	bash tests/summaryd/bootstrap_data_dir_safety_smoke.sh
	bash tests/summaryd/open_source_artifacts_smoke.sh
