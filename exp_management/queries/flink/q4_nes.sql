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
      MAX(
        CASE
          WHEN B.latency_ts >= A.latency_ts THEN B.latency_ts
          ELSE A.latency_ts
        END
      ) AS latency_ts
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
