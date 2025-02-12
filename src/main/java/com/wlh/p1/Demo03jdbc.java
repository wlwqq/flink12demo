package com.wlh.p1;

import com.wlh.pojo.Custom;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Demo03jdbc {
    public static void main(String[] args) throws Exception {
        // env
        Configuration jobConf = new Configuration();
        jobConf.setString(RestOptions.BIND_PORT, "18081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(jobConf);

        // source
        Properties kafkaSourceProps = new Properties();
        kafkaSourceProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaSourceProps.setProperty("group.id", "test");
        FlinkKafkaConsumer<String> sourceFunc = new FlinkKafkaConsumer<>("user_behavior", new SimpleStringSchema(), kafkaSourceProps);
        // sink
        SinkFunction<Custom> sinkFunc = JdbcSink.sink(
                "insert into books (id, title, author, price, qty) values (?,?,?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.name);
                    ps.setInt(2, t.age);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/mydatabase")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("user")
                        .withPassword("root")
                        .build()
        );
        // job
        env.addSource(sourceFunc)
                .map(new CustomMapFunction())
                .addSink(sinkFunc);
        env.execute("kafka-demo");
    }

    private static class CustomMapFunction implements MapFunction<String, Custom> {

        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Custom map(String value) throws Exception {
            return objectMapper.readValue(value, Custom.class);
        }
    }
}
