\set ON_ERROR_STOP on

\copy (select * from pg_flashback(:typed_null_expr, :'target_ts') order by :order_by) to :'csv_path' with (format csv, header true)
