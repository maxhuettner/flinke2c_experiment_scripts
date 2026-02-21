CREATE TABLE
  nexmark_q1 (
    auction BIGINT,
    bidder BIGINT,
    price DECIMAL(23, 3),
    `dateTime` TIMESTAMP(3),
    extra VARCHAR,
    latency_ts BIGINT
  )
WITH
  (
    'connector' = 'tcp-sink',
    'host' = '10.10.0.10',
    'port' = '9000'
  );

SET 'pipeline.object-reuse' = 'true';

INSERT INTO
  nexmark_q1
SELECT
  auction,
  bidder,
  0.908 * price as price, -- convert dollar to euro
  `dateTime`,
  extra,
  latency_ts
FROM
  bids;
