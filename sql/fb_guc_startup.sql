DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;

SELECT current_setting('pg_flashback.memory_limit');
\setenv PGHOST /tmp
\setenv PGDATABASE contrib_regression
\! env PGOPTIONS=-cpg_flashback.memory_limit=6GB psql -X -A -t -v ON_ERROR_STOP=1 -c "select current_setting('pg_flashback.memory_limit');"
\setenv PGHOST
\setenv PGDATABASE
SELECT current_setting('pg_flashback.memory_limit');
