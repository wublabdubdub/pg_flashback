\set ON_ERROR_STOP on
\if :{?toast_row_count}
\else
\set toast_row_count 4000
\endif
\if :{?toast_update_count}
\else
\set toast_update_count 2000
\endif
\if :{?toast_delete_count}
\else
\set toast_delete_count 500
\endif
\if :{?toast_insert_count}
\else
\set toast_insert_count 500
\endif
\if :{?toast_rollback_count}
\else
\set toast_rollback_count 1001
\endif

CREATE EXTENSION IF NOT EXISTS pg_flashback;
SELECT set_config('pg_flashback.archive_dir', current_setting('data_directory') || '/pg_wal', false);

DROP TABLE IF EXISTS fb_toast_scale_result;
DROP TABLE IF EXISTS fb_toast_scale_mark;
DROP TABLE IF EXISTS fb_toast_scale_truth;
DROP TABLE IF EXISTS fb_toast_scale_src CASCADE;

CREATE TABLE fb_toast_scale_src (
	id integer PRIMARY KEY,
	payload_a text,
	payload_b text,
	note text
);

ALTER TABLE fb_toast_scale_src ALTER COLUMN payload_a SET STORAGE EXTERNAL;
ALTER TABLE fb_toast_scale_src ALTER COLUMN payload_b SET STORAGE EXTERNAL;

CHECKPOINT;

INSERT INTO fb_toast_scale_src
SELECT g,
	   repeat(md5(g::text), 400),
	   repeat(md5((g + 100000)::text), 300),
	   'baseline'
FROM generate_series(1, :toast_row_count) AS g;

CREATE TABLE fb_toast_scale_truth AS
SELECT * FROM fb_toast_scale_src;

CHECKPOINT;

CREATE TABLE fb_toast_scale_mark AS
SELECT clock_timestamp() AS target_ts;

BEGIN;
UPDATE fb_toast_scale_src
SET payload_a = repeat(md5((id + 200000)::text), 400),
	note = 'update-a'
WHERE id BETWEEN (:toast_row_count / 4) + 1
            AND (:toast_row_count / 4) + :toast_update_count;
COMMIT;

BEGIN;
DELETE FROM fb_toast_scale_src
WHERE id BETWEEN (:toast_row_count / 4) + (:toast_update_count / 4) + 1
            AND (:toast_row_count / 4) + (:toast_update_count / 4) + :toast_delete_count;
COMMIT;

BEGIN;
INSERT INTO fb_toast_scale_src
SELECT g,
	   repeat(md5(g::text), 400),
	   repeat(md5((g + 100000)::text), 300),
	   'inserted'
FROM generate_series(:toast_row_count + 1, :toast_row_count + :toast_insert_count) AS g;
COMMIT;

BEGIN;
UPDATE fb_toast_scale_src
SET payload_b = repeat(md5((id + 300000)::text), 300),
	note = 'rollback-b'
WHERE id BETWEEN ((:toast_row_count * 3) / 4) - (:toast_rollback_count / 2)
            AND ((:toast_row_count * 3) / 4) - (:toast_rollback_count / 2) + :toast_rollback_count - 1;
ROLLBACK;

SELECT pg_flashback(
	'fb_toast_scale_result',
	'public.fb_toast_scale_src',
	(SELECT target_ts::text FROM fb_toast_scale_mark)
);

WITH truth_rows AS (
	SELECT id,
		   md5(payload_a) AS digest_a,
		   md5(payload_b) AS digest_b,
		   length(payload_a) AS len_a,
		   length(payload_b) AS len_b,
		   note
	FROM fb_toast_scale_truth
),
result_rows AS (
	SELECT id,
		   md5(payload_a) AS digest_a,
		   md5(payload_b) AS digest_b,
		   length(payload_a) AS len_a,
		   length(payload_b) AS len_b,
		   note
	FROM fb_toast_scale_result
),
diff_rows AS (
	(SELECT * FROM truth_rows EXCEPT SELECT * FROM result_rows)
	UNION ALL
	(SELECT * FROM result_rows EXCEPT SELECT * FROM truth_rows)
)
SELECT
	(SELECT count(*) FROM fb_toast_scale_truth) AS truth_count,
	(SELECT count(*) FROM fb_toast_scale_result) AS result_count,
	(SELECT count(*) FROM diff_rows) AS diff_count;

SELECT id, left(payload_a, 4) AS prefix_a, left(payload_b, 4) AS prefix_b, note
FROM fb_toast_scale_result
WHERE id IN (
	1,
	GREATEST(1, :toast_row_count / 3),
	GREATEST(1, (:toast_row_count * 2) / 3),
	:toast_row_count - 1
)
ORDER BY id;
