DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;

CREATE EXTENSION pg_flashback VERSION '0.1.0';

SELECT extversion = '0.1.0' AS at_old_version
FROM pg_extension
WHERE extname = 'pg_flashback';

SELECT to_regprocedure('pg_flashback(text,text,text)') IS NOT NULL AS has_old_entry,
	   to_regprocedure('pg_flashback(anyelement,text)') IS NULL AS no_new_entry;

ALTER EXTENSION pg_flashback UPDATE TO '0.2.0';

SELECT extversion = '0.2.0' AS at_new_version
FROM pg_extension
WHERE extname = 'pg_flashback';

SELECT to_regprocedure('pg_flashback(text,text,text)') IS NULL AS no_old_entry,
	   to_regprocedure('pg_flashback(anyelement,text)') IS NOT NULL AS has_new_entry;

SET pg_flashback.show_progress = off;

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

CREATE TABLE fb_extension_upgrade_target (
	id integer PRIMARY KEY,
	note text
);

INSERT INTO fb_extension_upgrade_target VALUES
	(1, 'before'),
	(2, 'before');

CREATE TABLE fb_extension_upgrade_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_extension_upgrade_mark VALUES (clock_timestamp());

INSERT INTO fb_extension_upgrade_target VALUES (3, 'after');
DELETE FROM fb_extension_upgrade_target WHERE id = 2;

SELECT *
FROM pg_flashback(
	NULL::public.fb_extension_upgrade_target,
	(SELECT target_ts::text FROM fb_extension_upgrade_mark)
)
ORDER BY id;
