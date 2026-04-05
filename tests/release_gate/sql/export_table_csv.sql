\set ON_ERROR_STOP on

begin isolation level repeatable read;
select to_char(transaction_timestamp() at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US+00') as capture_ts \gset
\copy (select * from :qualified_table order by :order_by) to :'csv_path' with (format csv, header true)
commit;
\echo capture_ts=:capture_ts
