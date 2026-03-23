\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS pg_flashback;
SET pg_flashback.archive_dest = :'archive_dest';
SET pg_flashback.ckwal_restore_dir = :'ckwal_dir';
SELECT set_config('fb_deep.row_count', :'row_count', false);
SELECT set_config('fb_deep.distinct_count', :'distinct_count', false);

TRUNCATE fb_deep_bag_01;

DO $$
DECLARE
	select_list text := 'bucket';
	expr text;
	i integer;
BEGIN
	FOR i IN 1..63 LOOP
		expr := CASE (i % 6)
			WHEN 1 THEN format('(bucket * %s)::bigint', i + 3)
			WHEN 2 THEN format('((bucket + %s) %% 50000)::integer', i)
			WHEN 3 THEN format('round(((bucket %% 50000)::numeric / 10.0) + %s, 2)', i)
			WHEN 4 THEN format('((bucket + %s) %% 2 = 0)', i)
			WHEN 5 THEN format('(timestamptz %L + ((bucket %% 50000) || %L)::interval)', '2026-01-01 00:00:00+08', ' seconds')
			ELSE format('rpad(((bucket + %s) %% 10000)::text, 12, %L)::char(12)', i, 'b')
		END;
		select_list := select_list || format(', %s as c%s', expr, lpad(i::text, 2, '0'));
	END LOOP;

	EXECUTE format(
		'insert into fb_deep_bag_01 select %s from (select ((g - 1) %% %s)::integer as bucket from generate_series(1, %s) g) s',
		select_list,
		current_setting('fb_deep.distinct_count'),
		current_setting('fb_deep.row_count'));
END;
$$;
