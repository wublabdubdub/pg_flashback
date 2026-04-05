\set ON_ERROR_STOP on

with rels as (
	select
		n.nspname as schema_name,
		c.relname as table_name,
		pg_total_relation_size(c.oid) as size_bytes
	from pg_class c
	join pg_namespace n
		on n.oid = c.relnamespace
	where n.nspname = :'schema_name'
	  and c.relkind = 'r'
)
select format('%s.%s|%s', schema_name, table_name, size_bytes)
from rels
order by size_bytes desc, table_name;
