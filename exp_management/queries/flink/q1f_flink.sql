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
    'host' = '10.10.0.10', -- 10.10.10.1
    'port' = '9000'
  );

ADD JAR 'file:///lib/flinke2c.jar';

CREATE TEMPORARY FUNCTION PriceGreaterThan AS 'org.example.flinke2c.PriceGreaterThan';

SET 'pipeline.object-reuse' = 'true';

INSERT INTO
  nexmark_q1
SELECT
  auction,
  bidder,
  price,
  `dateTime`,
  extra,
  latency_ts
FROM
  bids WHERE PriceGreaterThan(price);

SET 'table.exec.mini-batch.enabled' = 'false';