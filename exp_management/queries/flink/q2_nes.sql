CREATE TABLE nexmark_q2 (
  auction  BIGINT,
  price  BIGINT,
  latency_ts BIGINT
) WITH (
  'connector' = 'tcp-sink',
  'host' = '10.10.0.10',
  'port' = '9000'
);

-- Identical to q2.sql: every field it uses (auction, price, latency_ts) is
-- already present in the trimmed NES-comparable bids schema, so there's
-- nothing to drop or simplify here - same query, just run against
-- setup_nes.sql via source_schema: nes.
INSERT INTO nexmark_q2
SELECT auction, price, latency_ts FROM bids WHERE MOD(auction, 123) = 0;
