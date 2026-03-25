\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS pg_flashback;
SET pg_flashback.archive_dest = :'archive_dest';
SET pg_flashback.ckwal_restore_dir = :'ckwal_dir';
SET pg_flashback.debug_pg_wal_dir = :'debug_pg_wal_dir';

SELECT pg_flashback(
	:'result_table',
	:'source_table',
	(SELECT target_ts::text FROM fb_deep_markers WHERE label = :'marker_label')
);

WITH result_grouped AS (
	SELECT to_jsonb(t) AS row_doc, count(*) AS cnt
	  FROM :"result_table" t
	 GROUP BY 1
),
truth_grouped AS (
	SELECT to_jsonb(t) AS row_doc, count(*) AS cnt
	  FROM :"truth_table" t
	 GROUP BY 1
),
extra AS (
	SELECT * FROM result_grouped
	EXCEPT
	SELECT * FROM truth_grouped
),
missing AS (
	SELECT * FROM truth_grouped
	EXCEPT
	SELECT * FROM result_grouped
)
SELECT
	(SELECT count(*) FROM extra) AS extra_count,
	(SELECT count(*) FROM missing) AS missing_count,
	((SELECT count(*) FROM extra) = 0 AND (SELECT count(*) FROM missing) = 0) AS pass;
