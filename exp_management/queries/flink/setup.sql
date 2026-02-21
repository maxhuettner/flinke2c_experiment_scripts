CREATE TABLE
    persons (
        id BIGINT,
        name STRING,
        emailAddress STRING,
        creditCard STRING,
        city STRING,
        state STRING,
        `dateTime` TIMESTAMP(3),
        extra STRING,
        WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
    )
WITH
    (
        'connector' = 'tcp-source',
        'host' = '10.10.0.16',
        'port' = '10002'
    );

CREATE TABLE
    auctions (
        id BIGINT,
        itemName STRING,
        description STRING,
        initialBid BIGINT,
        reserve BIGINT,
        `dateTime` TIMESTAMP(3),
        expires TIMESTAMP(3),
        seller BIGINT,
        category BIGINT,
        extra STRING,
        WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
    )
WITH
    (
        'connector' = 'tcp-source',
        'host' = '10.10.0.16',
        'port' = '10001'
    );

CREATE TABLE
    bids (
        auction BIGINT,
        bidder BIGINT,
        price BIGINT,
        channel STRING,
        url STRING,
        `dateTime` TIMESTAMP(3),
        extra STRING,
        latency_ts BIGINT,
        WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
    )
WITH
    (
        'connector' = 'tcp-source',
        'host' = '10.10.0.16',
        'port' = '10000'
    );

SET 'table.exec.mini-batch.enabled' = 'false';