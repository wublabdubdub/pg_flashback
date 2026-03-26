\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS pg_flashback;
SET pg_flashback.archive_dest = :'archive_dest';
SELECT set_config('fb_deep.op_row_count', :'op_row_count', false);

BEGIN;
UPDATE fb_deep_keyed_01
   SET c01 = c01 + 700,
       c06 = 'SRC_D01_____'
 WHERE id % 7 = 0
   AND id <= :op_row_count;
COMMIT;
