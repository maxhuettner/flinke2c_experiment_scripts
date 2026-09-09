CREATE TABLE nexmark_q8 (
  id  BIGINT,
  stime  TIMESTAMP(3),
  latency_ts BIGINT
) WITH (
  'connector' = 'tcp-sink',
  'host' = '10.10.0.10',
  'port' = '9000'
);

-- Same as q8.sql, with two changes matching q8.txt:
-- - Drops P.name: not present in the trimmed NES-comparable persons schema
--   (setup_nes.sql) - NES's schema has never carried a text field anywhere
--   in this project, so this stays consistent rather than being the first.
-- - latency_ts uses only the auction side (MAX(A.latency_ts)), not
--   GREATEST(P.latency_ts, A.latency_ts): NES has no CASE WHEN/GREATEST, so
--   both sides compute the same (simpler) thing instead - same convention
--   as q4_nes.sql's bid-only latency.
INSERT INTO nexmark_q8
SELECT
  P.id,
  P.starttime,
  A.latency_ts
FROM (
  SELECT id, MAX(latency_ts) AS latency_ts, window_start AS starttime, window_end AS endtime
  FROM TABLE(TUMBLE(TABLE persons, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
  GROUP BY id, window_start, window_end
) P
JOIN (
  SELECT seller, MAX(latency_ts) AS latency_ts, window_start AS starttime, window_end AS endtime
  FROM TABLE(TUMBLE(TABLE auctions, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
  GROUP BY seller, window_start, window_end
) A
ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;
