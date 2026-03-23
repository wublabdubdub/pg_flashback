DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;

CREATE UNLOGGED TABLE fb_rel_unlogged (id integer);
SELECT fb_check_relation('fb_rel_unlogged'::regclass);

CREATE TEMP TABLE fb_rel_temp (id integer);
SELECT fb_check_relation('fb_rel_temp'::regclass);

CREATE MATERIALIZED VIEW fb_rel_matview AS SELECT 1 AS id;
SELECT fb_check_relation('fb_rel_matview'::regclass);

CREATE TABLE fb_rel_parent (id integer) PARTITION BY RANGE (id);
SELECT fb_check_relation('fb_rel_parent'::regclass);

CREATE VIEW fb_rel_view AS SELECT 1 AS id;
SELECT fb_check_relation('fb_rel_view'::regclass);

CREATE SEQUENCE fb_rel_seq;
SELECT fb_check_relation('fb_rel_seq'::regclass);

CREATE TABLE fb_rel_index_base (id integer);
CREATE INDEX fb_rel_idx ON fb_rel_index_base(id);
SELECT fb_check_relation('fb_rel_idx'::regclass);

SELECT fb_check_relation('pg_class'::regclass);

CREATE TABLE fb_rel_toast (v text);
SELECT fb_check_relation((SELECT reltoastrelid FROM pg_class WHERE oid = 'fb_rel_toast'::regclass)::regclass);
