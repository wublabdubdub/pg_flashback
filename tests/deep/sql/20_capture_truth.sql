\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS pg_flashback;
SET pg_flashback.archive_dest = :'archive_dest';
SELECT set_config('fb_deep.source_table', :'source_table', false);
SELECT set_config('fb_deep.truth_table', :'truth_table', false);

DELETE FROM fb_deep_markers WHERE label = :'marker_label';

INSERT INTO fb_deep_markers(label, target_ts, source_table, truth_table)
VALUES (:'marker_label', clock_timestamp(), :'source_table', :'truth_table');

DO $$
BEGIN
	EXECUTE format('drop table if exists %I', current_setting('fb_deep.truth_table'));
	EXECUTE format('create table %I as table %I',
				   current_setting('fb_deep.truth_table'),
				   current_setting('fb_deep.source_table'));
	EXECUTE format('alter table %I set (autovacuum_enabled = false, toast.autovacuum_enabled = false)',
				   current_setting('fb_deep.truth_table'));
END;
$$;
