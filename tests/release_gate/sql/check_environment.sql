\pset tuples_only on
\pset format unaligned

SELECT 'archive_mode=' || current_setting('archive_mode');
SELECT 'archive_command=' || current_setting('archive_command');
SELECT 'matched_archive_dir=' ||
       CASE
         WHEN position(:'expected_archive_dir' in current_setting('archive_command')) > 0
           THEN :'expected_archive_dir'
         ELSE ''
       END;
