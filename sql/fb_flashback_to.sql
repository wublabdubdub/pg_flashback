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

CREATE TABLE fb_flashback_to_target (
	id integer PRIMARY KEY,
	code text UNIQUE,
	amount integer NOT NULL,
	note text
);

CHECKPOINT;

INSERT INTO fb_flashback_to_target VALUES
	(1, 'code-1', 10, 'baseline-1'),
	(2, 'code-2', 20, 'baseline-2'),
	(3, 'code-3', 30, 'baseline-3');

CREATE TABLE fb_flashback_to_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_flashback_to_mark VALUES (clock_timestamp());

UPDATE fb_flashback_to_target
SET amount = amount + 100,
	note = note || '-later'
WHERE id = 1;

UPDATE fb_flashback_to_target
SET id = 33,
	code = 'code-33',
	note = 'moved-key'
WHERE id = 3;

DELETE FROM fb_flashback_to_target
WHERE id = 2;

INSERT INTO fb_flashback_to_target VALUES
	(4, 'code-4', 40, 'after-mark');

SET client_min_messages = warning;

SELECT pg_flashback_to(
	'public.fb_flashback_to_target'::regclass,
	(SELECT target_ts::text FROM fb_flashback_to_mark)
);

TABLE fb_flashback_to_target
ORDER BY id;

CREATE TABLE fb_flashback_to_fk_ref (
	id integer PRIMARY KEY
);

INSERT INTO fb_flashback_to_fk_ref VALUES (1), (2);

CREATE TABLE fb_flashback_to_fk_target (
	id integer PRIMARY KEY REFERENCES fb_flashback_to_fk_ref(id),
	payload text
);

INSERT INTO fb_flashback_to_fk_target VALUES (1, 'seed');

CREATE TABLE fb_flashback_to_fk_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_flashback_to_fk_mark VALUES (clock_timestamp());

UPDATE fb_flashback_to_fk_target
SET payload = 'after-mark'
WHERE id = 1;

SELECT pg_flashback_to(
	'public.fb_flashback_to_fk_target'::regclass,
	(SELECT target_ts::text FROM fb_flashback_to_fk_mark)
);

CREATE TABLE fb_flashback_to_trg_target (
	id integer PRIMARY KEY,
	payload text
);

CREATE FUNCTION fb_flashback_to_noop_trg()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
	RETURN NEW;
END;
$$;

CREATE TRIGGER fb_flashback_to_target_noop_trg
BEFORE INSERT OR UPDATE ON fb_flashback_to_trg_target
FOR EACH ROW
EXECUTE FUNCTION fb_flashback_to_noop_trg();

INSERT INTO fb_flashback_to_trg_target VALUES (1, 'seed');

CREATE TABLE fb_flashback_to_trg_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_flashback_to_trg_mark VALUES (clock_timestamp());

UPDATE fb_flashback_to_trg_target
SET payload = 'after-mark'
WHERE id = 1;

SELECT pg_flashback_to(
	'public.fb_flashback_to_trg_target'::regclass,
	(SELECT target_ts::text FROM fb_flashback_to_trg_mark)
);

SELECT 'fb_flashback_to_done' AS status;

RESET client_min_messages;
