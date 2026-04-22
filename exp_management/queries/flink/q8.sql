CREATE TABLE nexmark_q8 (
  id  BIGINT,
  name  VARCHAR,
  stime  TIMESTAMP(3)
) WITH (
  'connector' = 'tcp-sink',
  'host' = '10.10.0.10',
  'port' = '9000'
);

INSERT INTO nexmark_q8
SELECT P.id, P.name, P.starttime
FROM (
  SELECT id, name,
        window_start AS starttime,
        window_end AS endtime
  FROM TABLE(
            TUMBLE(TABLE persons, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
  GROUP BY id, name, window_start, window_end
) P
JOIN (
  SELECT seller,
        window_start AS starttime,
        window_end AS endtime
  FROM TABLE(
        TUMBLE(TABLE auctions, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
  GROUP BY seller, window_start, window_end
) A
ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;