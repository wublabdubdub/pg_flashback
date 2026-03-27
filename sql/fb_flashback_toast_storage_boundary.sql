DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;
SET pg_flashback.show_progress = off;

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

CREATE TABLE fb_flashback_toast_storage_boundary_target (
	id integer PRIMARY KEY,
	payload text NOT NULL
);

CREATE TABLE fb_flashback_toast_storage_boundary_mark (
	target_ts timestamptz NOT NULL
);

CREATE TABLE fb_flashback_toast_storage_boundary_size (
	label text PRIMARY KEY,
	bytes bigint NOT NULL
);

CREATE TABLE fb_flashback_toast_storage_boundary_diag (
	errmsg_text text NOT NULL,
	detail_text text
);

INSERT INTO fb_flashback_toast_storage_boundary_target
SELECT g,
	   (SELECT string_agg(md5((g * 100000 + s)::text), '')
		FROM generate_series(1, 128) AS s)
FROM generate_series(1, 2000) AS g;

INSERT INTO fb_flashback_toast_storage_boundary_mark
VALUES (clock_timestamp());

INSERT INTO fb_flashback_toast_storage_boundary_size
SELECT 'before_vacuum',
	   pg_relation_size(reltoastrelid)
FROM pg_class
WHERE oid = 'fb_flashback_toast_storage_boundary_target'::regclass;

UPDATE fb_flashback_toast_storage_boundary_target
SET payload = repeat('small', 8)
WHERE id > 1000;

VACUUM fb_flashback_toast_storage_boundary_target;

INSERT INTO fb_flashback_toast_storage_boundary_size
SELECT 'after_vacuum',
	   pg_relation_size(reltoastrelid)
FROM pg_class
WHERE oid = 'fb_flashback_toast_storage_boundary_target'::regclass;

SELECT (SELECT bytes
		FROM fb_flashback_toast_storage_boundary_size
		WHERE label = 'before_vacuum') >
	   (SELECT bytes
		FROM fb_flashback_toast_storage_boundary_size
		WHERE label = 'after_vacuum') AS toast_shrank;

DO $$
DECLARE
	errmsg_text text;
	detail_text text;
BEGIN
	BEGIN
		PERFORM count(*)
		FROM pg_flashback(
			NULL::public.fb_flashback_toast_storage_boundary_target,
			(SELECT target_ts::text
			 FROM fb_flashback_toast_storage_boundary_mark)
		);
		RAISE EXCEPTION 'expected toast boundary flashback to fail';
	EXCEPTION
		WHEN feature_not_supported THEN
			GET STACKED DIAGNOSTICS
				errmsg_text = MESSAGE_TEXT,
				detail_text = PG_EXCEPTION_DETAIL;
			INSERT INTO fb_flashback_toast_storage_boundary_diag
			VALUES (errmsg_text, detail_text);
	END;
END;
$$;

SELECT errmsg_text = 'fb does not support WAL windows containing storage_change operations' AS message_ok,
	   detail_text LIKE '%scope=toast%' AS scope_ok,
	   detail_text LIKE '%operation=smgr_truncate%' AS operation_ok,
	   detail_text ~ 'xid=[0-9]+' AS xid_ok,
	   detail_text ~ 'commit_ts=[0-9]{4}-[0-9]{2}-[0-9]{2} ' AS commit_ts_ok
FROM fb_flashback_toast_storage_boundary_diag;
