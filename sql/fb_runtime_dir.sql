DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;
SELECT fb_runtime_dir_debug();
SELECT fb_runtime_dir_debug() = format('base=%s/pg_flashback runtime=true recovered=true meta=true', current_setting('data_directory')) AS matches_contract;
