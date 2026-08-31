CREATE TABLE
  nexmark_q4 (
    id BIGINT,
    final BIGINT,
    latency_ts BIGINT
  )
WITH (
  'connector' = 'tcp-sink',
  'host' = '10.10.0.10',
  'port' = '9000'
);

-- Identical structure to plain q4.sql (same continuous, unbounded interval
-- join and aggregation - proven reliable, no hang risk). Three different
-- windowed rewrites of this file (event-time TVF join, classic
-- "GROUP BY x, TUMBLE(col,size)", TVF-over-subquery) all hung the same way:
-- the submitted job never appeared in Flink's job list at all, which points
-- to Calcite's planner getting stuck on the nested-subquery-plus-windowing-
-- TVF query shape rather than any runtime/execution issue. NES's engine
-- fundamentally requires a window and Flink's planner can't reliably handle
-- windowing this join, so an exact computational-shape match isn't
-- achievable here - this keeps Flink's real, working continuous-aggregate
-- behavior instead. See exp_management/queries/nes/q4.txt for NES's side.
--
-- latency_ts uses only the bid side (MAX(B.latency_ts)), not
-- GREATEST(A.latency_ts, B.latency_ts): NES has no CASE WHEN/GREATEST, so
-- its q4.txt combines A/B latency via an ABS()-based max() workaround that
-- turned out to produce bad values in practice. Matching NES's simplified,
-- bid-only definition here keeps both sides computing the same thing rather
-- than Flink doing the more "correct" GREATEST and NES doing something else.
INSERT INTO
  nexmark_q4
SELECT
  Q.category,
  AVG(Q.final),
  MAX(Q.latency_ts)
FROM
  (
    SELECT
      MAX(B.price) AS final,
      A.category,
      MAX(B.latency_ts) AS latency_ts
    FROM
      auctions A,
      bids B
    WHERE
      A.id = B.auction
      AND B.`dateTime` BETWEEN A.`dateTime` AND A.expires
    GROUP BY
      A.id,
      A.category
  ) Q
GROUP BY
  Q.category;
