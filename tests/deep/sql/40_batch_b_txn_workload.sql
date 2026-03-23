\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS pg_flashback;
SET pg_flashback.archive_dest = :'archive_dest';
SET pg_flashback.ckwal_restore_dir = :'ckwal_dir';

BEGIN;
UPDATE fb_deep_keyed_01
   SET c01 = c01 + 50000,
       c06 = 'TXN_B01_____'
 WHERE id BETWEEN 1 AND (:row_count / 10);
COMMIT;

BEGIN;
UPDATE fb_deep_keyed_01
   SET c02 = c02 + 99,
       c06 = 'TXN_BRB_____'
 WHERE id BETWEEN (:row_count / 10) + 1 AND (:row_count / 5);

DELETE FROM fb_deep_keyed_01
 WHERE id % 97 = 0
   AND id <= :row_count;
ROLLBACK;

BEGIN;
UPDATE fb_deep_keyed_01
   SET c03 = c03 + 9.99,
       c05 = c05 + interval '9 seconds'
 WHERE id % 23 = 0
   AND id <= :row_count;
COMMIT;
