\set ON_ERROR_STOP on

drop table if exists :ctas_table;
create unlogged table :ctas_table as
select *
from pg_flashback(:typed_null_expr, :'target_ts');
