-- 输入数据demo
-- {"user_name":"wlh", "score": 12, "ts":1}

CREATE TABLE KafkaTable
(
    `user_name` varchar,
    `score`     BIGINT,
    `ts`        BIGINT
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
    `user_name` varchar,
    `score`     BIGINT,
    `ts`        BIGINT
) WITH (
      'connector' = 'print'
      )
;

insert into PrintTable
select user_name, score, ts
from (select *, row_number() over (partition by user_name order by ts desc) as rn
      from KafkaTable) t
where rn = 1;