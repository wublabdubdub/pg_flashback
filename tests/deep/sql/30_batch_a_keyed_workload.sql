\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS pg_flashback;
SET pg_flashback.archive_dest = :'archive_dest';
SET pg_flashback.ckwal_restore_dir = :'ckwal_dir';
SELECT set_config('fb_deep.row_count', :'row_count', false);
SELECT set_config('fb_deep.insert_count', :'insert_count', false);

BEGIN;
UPDATE fb_deep_keyed_01
   SET c01 = c01 + 1000,
       c02 = c02 + 7,
       c03 = c03 + 1.11,
       c04 = NOT c04,
       c05 = c05 + interval '5 minutes',
       c06 = 'UPD_A01_____'
 WHERE id % 3 = 0
   AND id <= :row_count;
COMMIT;

BEGIN;
DELETE FROM fb_deep_keyed_01
 WHERE id % 11 = 0
   AND id <= :row_count;
COMMIT;

DO $$
DECLARE
	select_list text := 'g';
	expr text;
	i integer;
BEGIN
	FOR i IN 1..63 LOOP
		expr := CASE (i % 6)
			WHEN 1 THEN format('(g * %s)::bigint', i + 11)
			WHEN 2 THEN format('((g + %s) %% 100000)::integer', i + 13)
			WHEN 3 THEN format('round(((g %% 100000)::numeric / 77.0) + %s, 2)', i)
			WHEN 4 THEN format('((g + %s) %% 2 = 1)', i)
			WHEN 5 THEN format('(timestamptz %L + ((g %% 100000) || %L)::interval)', '2026-02-01 00:00:00+08', ' seconds')
			ELSE format('%L::char(12)', 'INS_A01_____')
		END;
		select_list := select_list || format(', %s as c%s', expr, lpad(i::text, 2, '0'));
	END LOOP;

	EXECUTE format(
		'insert into fb_deep_keyed_01 select %s from generate_series(%s, %s) g',
		select_list,
		current_setting('fb_deep.row_count')::bigint + 1,
		current_setting('fb_deep.row_count')::bigint + current_setting('fb_deep.insert_count')::bigint);
END;
$$;

BEGIN;
UPDATE fb_deep_keyed_01
   SET c01 = c01 - 5,
       c06 = 'ROLLBACK____'
 WHERE id % 19 = 0
   AND id <= :row_count;
ROLLBACK;
