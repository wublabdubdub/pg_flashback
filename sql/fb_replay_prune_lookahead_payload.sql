SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS pg_flashback;
RESET client_min_messages;
SET pg_flashback.show_progress = off;

CREATE FUNCTION fb_replay_prune_lookahead_payload_contract_debug(regclass, timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_prune_lookahead_payload_contract_debug'
LANGUAGE C STRICT;

CREATE TABLE fb_replay_prune_payload_t(
    id int PRIMARY KEY,
    payload text
);

SELECT clock_timestamp() AS target_ts \gset

INSERT INTO fb_replay_prune_payload_t
VALUES
    (1, repeat('a', 256)),
    (2, repeat('b', 256));

UPDATE fb_replay_prune_payload_t
SET payload = repeat('c', 256)
WHERE id = 2;

SELECT pg_switch_wal() AS switched_lsn \gset

SELECT fb_replay_prune_lookahead_payload_contract_debug(
    'fb_replay_prune_payload_t'::regclass,
    :'target_ts'::timestamptz
);

DROP TABLE fb_replay_prune_payload_t;
DROP FUNCTION fb_replay_prune_lookahead_payload_contract_debug(regclass, timestamptz);
