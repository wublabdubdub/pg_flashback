\set ON_ERROR_STOP on

select format('drop database if exists %I', :'dbname') as ddl \gexec
select format('create database %I', :'dbname') as ddl \gexec
