CREATE TABLE nexmark_q2 (
  auction  BIGINT,
  price  BIGINT,
  latency_ts BIGINT
) WITH (
  'connector' = 'tcp-sink',
  'host' = '10.10.0.10',
  'port' = '9000'
);

INSERT INTO nexmark_q2
SELECT auction, price, latency_ts FROM bids WHERE MOD(auction, 123) = 0;
