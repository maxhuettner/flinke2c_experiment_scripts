CREATE TABLE
  nexmark_q4 (
    category BIGINT,
    id BIGINT,
    final BIGINT,
    latency_ts BIGINT
  )
WITH (
  'connector' = 'tcp-sink',
  'host' = '10.10.0.10',
  'port' = '9000'
);

-- Matches NES's q4.txt: same 24h-windowed join (TUMBLE on both sides,
-- joined on matching window boundaries, real interval condition kept as a
-- filter within the bucket) so this emits one snapshot per 24h window close
-- instead of updating on every bid, same as NES's coordinator-side
-- EventTime/IngestionTime window pair. Also matches NES's q4.txt in two
-- other respects:
-- - Stops at the first aggregation stage (MAX(price) per auction id +
--   category) instead of the real Nexmark q4's second AVG-by-category
--   stage, which NES's engine doesn't compute.
-- - latency_ts uses only the bid side (MAX(B.latency_ts)), not
--   GREATEST(A.latency_ts, B.latency_ts): NES has no CASE WHEN/GREATEST, so
--   both sides compute the same (simpler) thing instead.
--
-- Earlier windowed rewrites of this query hung Calcite's planner (job never
-- appeared in Flink's job list at all). That's consistent with JobManager
-- memory being undersized at the time (600m fallback for nodes without a
-- known AWS instance spec - since raised to 1024m, see _worker_memory_mb /
-- jm_memory_mb in cli/experiment.py): a starved JobManager can GC-thrash
-- during query compilation of a heavier windowed-TVF plan without ever
-- producing a JobGraph, which looks identical to a planner hang from the
-- outside. Now that JobManager memory scales with the node, this windowed
-- join runs the same as any other query submission.
INSERT INTO
  nexmark_q4
SELECT
  A.category,
  A.id,
  MAX(B.price) AS final,
  MAX(B.latency_ts) AS latency_ts
FROM
  TABLE (
    TUMBLE(
      TABLE auctions,
      DESCRIPTOR(`dateTime`),
      INTERVAL '24' HOUR
    )
  ) AS A
  JOIN TABLE (
    TUMBLE(
      TABLE bids,
      DESCRIPTOR(`dateTime`),
      INTERVAL '24' HOUR
    )
  ) AS B
  ON A.window_start = B.window_start
  AND A.window_end = B.window_end
  AND A.id = B.auction
WHERE
  B.`dateTime` BETWEEN A.`dateTime` AND A.expires
GROUP BY
  A.window_start,
  A.window_end,
  A.id,
  A.category;
