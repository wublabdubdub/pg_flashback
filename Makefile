EXTENSION = pg_flashback
MODULE_big = pg_flashback
OBJS = \
	src/fb_entry.o \
	src/fb_guc.o \
	src/fb_catalog.o \
	src/fb_error.o \
	src/fb_wal.o \
	src/fb_decode.o \
	src/fb_replay.o \
	src/fb_reverse_ops.o \
	src/fb_apply_keyed.o \
	src/fb_apply_bag.o \
	src/fb_export.o \
	src/fb_toast.o \
	src/fb_compat_pg18.o

DATA = sql/pg_flashback--0.1.0.sql
REGRESS = fb_smoke fb_relation_gate fb_relation_unsupported fb_runtime_gate fb_wal_scan fb_recordref fb_replay fb_flashback_keyed fb_flashback_bag fb_flashback_materialize fb_create_flashback_table fb_memory_limit

PG_CONFIG ?= /home/18pg/local/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

override CPPFLAGS += -I$(CURDIR)/include

ifdef USE_PGXS
include $(PGXS)
else
include $(PGXS)
endif
