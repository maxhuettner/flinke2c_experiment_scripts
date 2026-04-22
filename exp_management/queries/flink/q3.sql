CREATE TABLE nexmark_q3 (
  name  VARCHAR,
  city  VARCHAR,
  state  VARCHAR,
  id  BIGINT
) WITH (
  'connector' = 'tcp-sink',
  'host' = '10.10.0.10',
  'port' = '9000'
);

INSERT INTO nexmark_q3
SELECT
    P.name, P.city, P.state, A.id
FROM
    auctions AS A INNER JOIN persons AS P on A.seller = P.id
WHERE
    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');