CREATE TABLE nexmark_q6 (
  seller BIGINT,
  avg_price  BIGINT
) WITH (
  'connector' = 'tcp-sink',
  'host' = '10.10.0.10',
  'port' = '9000'
);

-- TODO: this query is not supported yet in Flink SQL, because the OVER WINDOW operator doesn't
--  support to consume retractions.
INSERT INTO nexmark_q6
SELECT
    Q.seller,
    AVG(Q.price) OVER
        (PARTITION BY Q.seller ORDER BY Q.`dateTime` ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY A.id, A.seller ORDER BY B.price DESC) AS rownum
    FROM (SELECT A.id, A.seller, B.price, B.`dateTime`
        FROM auctions AS A,
            bids AS B
        WHERE A.id = B.auction
            and B.`dateTime` between A.`dateTime` and A.expires)
    WHERE rownum <= 1
) AS Q;