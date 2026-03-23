\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS pg_flashback;
SET pg_flashback.archive_dest = :'archive_dest';
SET pg_flashback.ckwal_restore_dir = :'ckwal_dir';

BEGIN;
UPDATE fb_deep_keyed_01
   SET c01 = c01 + 700,
       c06 = 'SRC_D01_____'
 WHERE id % 7 = 0
   AND id <= :row_count;
COMMIT;
