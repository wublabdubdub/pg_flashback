\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS pg_flashback;
SET pg_flashback.archive_dest = :'archive_dest';
SET pg_flashback.ckwal_restore_dir = :'ckwal_dir';
SELECT set_config('fb_deep.distinct_count', :'distinct_count', false);
SELECT set_config('fb_deep.insert_count', :'insert_count', false);

BEGIN;
UPDATE fb_deep_bag_01
   SET c01 = c01 + 123,
       c02 = c02 + 5,
       c03 = c03 + 0.55,
       c04 = NOT c04,
       c06 = 'UPD_C01_____'
 WHERE bucket % 4 = 0;
COMMIT;

BEGIN;
DELETE FROM fb_deep_bag_01
 WHERE bucket % 13 = 0;
COMMIT;

DO $$
DECLARE
	select_list text := 'bucket';
	expr text;
	i integer;
BEGIN
	FOR i IN 1..63 LOOP
		expr := CASE (i % 6)
			WHEN 1 THEN format('(bucket * %s)::bigint', i + 5)
			WHEN 2 THEN format('((bucket + %s) %% 50000)::integer', i + 2)
			WHEN 3 THEN format('round(((bucket %% 50000)::numeric / 11.0) + %s, 2)', i)
			WHEN 4 THEN format('((bucket + %s) %% 2 = 0)', i + 1)
			WHEN 5 THEN format('(timestamptz %L + ((bucket %% 50000) || %L)::interval)', '2026-02-10 00:00:00+08', ' seconds')
			ELSE format('%L::char(12)', 'INS_C01_____')
		END;
		select_list := select_list || format(', %s as c%s', expr, lpad(i::text, 2, '0'));
	END LOOP;

	EXECUTE format(
		'insert into fb_deep_bag_01 select %s from (select ((g - 1) %% %s)::integer as bucket from generate_series(1, %s) g) s',
		select_list,
		current_setting('fb_deep.distinct_count'),
		current_setting('fb_deep.insert_count'));
END;
$$;

BEGIN;
UPDATE fb_deep_bag_01
   SET c02 = c02 + 1,
       c06 = 'ROLL_C_____'
 WHERE bucket % 17 = 0;
ROLLBACK;
