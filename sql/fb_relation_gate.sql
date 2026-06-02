DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;

CREATE TABLE fb_rel_pk (id integer PRIMARY KEY, v text);
SELECT fb_check_relation('fb_rel_pk'::regclass);

CREATE TABLE fb_rel_bag (v text);
CREATE INDEX fb_rel_bag_v_idx ON fb_rel_bag(v);
SELECT fb_check_relation('fb_rel_bag'::regclass);

CREATE TABLE fb_rel_unique (code text UNIQUE, v text);
SELECT fb_check_relation('fb_rel_unique'::regclass);

CREATE TABLE fb_rel_plain (id integer);
SELECT fb_check_relation('fb_rel_plain'::regclass);
