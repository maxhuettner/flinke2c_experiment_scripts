CREATE TABLE nexmark_q7 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR,
  latency_ts BIGINT
) WITH (
  'connector' = 'tcp-sink',
  'host' = '10.10.0.10',
  'port' = '9000'
);

INSERT INTO nexmark_q7
SELECT B.auction, B.price, B.bidder, B.`dateTime`, B.extra, B.latency_ts
from bids B
JOIN (
  SELECT MAX(price) AS maxprice, window_end as `dateTime`
  FROM TABLE(
          TUMBLE(TABLE bids, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
  GROUP BY window_start, window_end
) B1
ON B.price = B1.maxprice
WHERE B.`dateTime` BETWEEN B1.`dateTime`  - INTERVAL '10' SECOND AND B1.`dateTime`;
