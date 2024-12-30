-- 输入数据demo
-- >    {"key1":"a", "val1":"1", "val2": "2024-12-27 22:11:00.000"}
-- >    {"key1":"a", "val1":"2", "val2": "2024-12-27 23:11:00.000"}
-- >    {"key1":"a", "val1":"3", "val2": "2024-12-27 22:10:00.000"}
-- >    {"key1":"a", "val1":"1", "val2": "2024-12-27 22:11:00.000"}


CREATE TABLE KafkaTable
(
    `key1` varchar,
    `val1` BIGINT,
    `val2` timestamp
) WITH (
      'connector' = 'kafka',
      'topic' = 'user_behavior',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'json'
      )
;

CREATE TABLE PrintTable
(
    `key1`     varchar,
    `cnt`      BIGINT,
    `val1_sum` bigint,
    `val2_max` timestamp
) WITH (
      'connector' = 'print'
      )
;


insert into PrintTable
select key1,
       count(*)  as cnt,
       sum(val1) as val1_sum,
       max(val2) as val2_max
from KafkaTable
group by key1;