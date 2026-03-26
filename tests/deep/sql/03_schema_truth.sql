\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS pg_flashback;
SET pg_flashback.archive_dest = :'archive_dest';

DROP TABLE IF EXISTS fb_deep_markers;
CREATE TABLE fb_deep_markers (
	label text PRIMARY KEY,
	target_ts timestamptz NOT NULL,
	source_table text NOT NULL,
	truth_table text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE fb_deep_markers SET (autovacuum_enabled = false);
