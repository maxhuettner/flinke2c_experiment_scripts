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
    'port' = '9000',
    'parallelism' = '1'
  );

ADD JAR 'file:///lib/flinke2c.jar';

CREATE TEMPORARY FUNCTION CurrencyConversionFunction AS 'org.example.flinke2c.CurrencyConversionFunction';

SET 'pipeline.object-reuse' = 'true';
SET 'table.exec.mini-batch.enabled' = 'false';
SET 'pipeline.operator-chaining.enabled'='false';

SET 'table.exec.external-runtime.chain-only.enabled' = 'true';
SET 'table.exec.external-runtime.function-class' = 'org.example.flinke2c.CurrencyConversionFunction';
SET 'table.exec.external-runtime.conf.org.example.flinke2c.CurrencyConversionFunction' =
  'runtimes=10.10.0.10:9001,10.10.0.10:9002;parallelism=auto;';

INSERT INTO
  nexmark_q1
SELECT
  auction,
  bidder,
  CurrencyConversionFunction(price, auction, bidder, channel, url, `dateTime`, extra).price as price, -- convert dollar to euro
  `dateTime`,
  extra,
  latency_ts
FROM
  bids;
