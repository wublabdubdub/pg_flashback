\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS pg_flashback;
SET pg_flashback.archive_dest = :'archive_dest';
SET pg_flashback.ckwal_restore_dir = :'ckwal_dir';
SELECT set_config('fb_deep.row_count', :'row_count', false);
SELECT set_config('fb_deep.op_row_count', :'op_row_count', false);
SELECT set_config('fb_deep.insert_count', :'insert_count', false);
SELECT set_config('fb_deep.stress_rounds', :'stress_rounds', false);

DO $$
DECLARE
	i integer;
	start_id bigint;
	end_id bigint;
	select_list text;
	expr text;
	colno integer;
BEGIN
	FOR i IN 1..current_setting('fb_deep.stress_rounds')::integer LOOP
		start_id := (current_setting('fb_deep.row_count')::bigint + 1) +
			((i - 1) * current_setting('fb_deep.insert_count')::bigint);
		end_id := start_id + current_setting('fb_deep.insert_count')::bigint - 1;

		EXECUTE format(
			'update fb_deep_keyed_01 set c01 = c01 + %s, c02 = c02 + %s, c06 = %L where id %% 5 = %s and id <= %s',
			i * 10,
			i,
			rpad('STRESS' || i::text, 12, '_'),
			i % 5,
			current_setting('fb_deep.op_row_count'));

		EXECUTE format(
			'delete from fb_deep_keyed_01 where id %% 31 = %s and id <= %s',
			i % 31,
			current_setting('fb_deep.op_row_count'));

		select_list := 'g';
		FOR colno IN 1..63 LOOP
			expr := CASE (colno % 6)
				WHEN 1 THEN format('(g * %s)::bigint', colno + (i * 3))
				WHEN 2 THEN format('((g + %s) %% 100000)::integer', colno + i)
				WHEN 3 THEN format('round(((g %% 100000)::numeric / 13.0) + %s, 2)', colno + i)
				WHEN 4 THEN format('((g + %s) %% 2 = 0)', colno + i)
				WHEN 5 THEN format('(timestamptz %L + ((g %% 100000) || %L)::interval)', '2026-03-01 00:00:00+08', ' seconds')
				ELSE format('%L::char(12)', rpad('STRS' || i::text, 12, '_'))
			END;
			select_list := select_list || format(', %s as c%s', expr, lpad(colno::text, 2, '0'));
		END LOOP;

		EXECUTE format(
			'insert into fb_deep_keyed_01 select %s from generate_series(%s, %s) g',
			select_list,
			start_id,
			end_id);
	END LOOP;
END;
$$;
