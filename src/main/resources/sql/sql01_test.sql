CREATE TABLE KafkaTable
(
    `user_id`  BIGINT,
    `item_id`  BIGINT,
    `behavior` STRING,
    `ts`       TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
      'connector' = 'kafka',
      'topic' = 'user_behavior',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'csv'
      )
;

CREATE TABLE PrintTable
(
    `user_id`  BIGINT,
    `item_id`  BIGINT,
    `behavior` STRING,
    `ts`       TIMESTAMP(3)
) WITH (
      'connector' = 'print'
      )
;

insert into PrintTable
select user_id, item_id, behavior, ts
from KafkaTable;