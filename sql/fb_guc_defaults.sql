DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;

SELECT current_setting('pg_flashback.memory_limit');
SELECT current_setting('pg_flashback.spill_mode');
SELECT current_setting('pg_flashback.parallel_workers');
SET pg_flashback.memory_limit = '2mb';
SELECT current_setting('pg_flashback.memory_limit');
SET pg_flashback.memory_limit = '1gb';
SELECT current_setting('pg_flashback.memory_limit');
SET pg_flashback.memory_limit = '8gb';
SELECT current_setting('pg_flashback.memory_limit');
SET pg_flashback.memory_limit = '32gb';
SELECT current_setting('pg_flashback.memory_limit');
SET pg_flashback.spill_mode = 'memory';
SELECT current_setting('pg_flashback.spill_mode');
SET pg_flashback.spill_mode = 'disk';
SELECT current_setting('pg_flashback.spill_mode');
SET pg_flashback.parallel_workers = 4;
SELECT current_setting('pg_flashback.parallel_workers');
SET pg_flashback.parallel_workers = 0;
SELECT current_setting('pg_flashback.parallel_workers');
SET pg_flashback.spill_mode = 'badmode';
SET pg_flashback.memory_limit = '33gb';
SET pg_flashback.parallel_workers = -1;
SET pg_flashback.runtime_retention = '1h';
SET pg_flashback.recovered_wal_retention = '2h';
SET pg_flashback.meta_retention = '3h';
SET pg_flashback.enable_parallel_workers = on;
SET pg_flashback.parallel_segment_scan = on;
SET pg_flashback.show_process = off;
