\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS pg_flashback;
SET pg_flashback.archive_dest = :'archive_dest';
SET pg_flashback.ckwal_restore_dir = :'ckwal_dir';

DROP TABLE IF EXISTS fb_deep_keyed_01;

DO $$
DECLARE
	cols text := 'id bigint primary key';
	col_type text;
	i integer;
BEGIN
	FOR i IN 1..63 LOOP
		col_type := CASE (i % 6)
			WHEN 1 THEN 'bigint not null'
			WHEN 2 THEN 'integer not null'
			WHEN 3 THEN 'numeric(12,2) not null'
			WHEN 4 THEN 'boolean not null'
			WHEN 5 THEN 'timestamptz not null'
			ELSE 'char(12) not null'
		END;
		cols := cols || format(', c%s %s', lpad(i::text, 2, '0'), col_type);
	END LOOP;

	EXECUTE format('create table fb_deep_keyed_01 (%s)', cols);
END;
$$;
