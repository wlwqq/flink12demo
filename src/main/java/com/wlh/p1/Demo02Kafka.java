package com.wlh.p1;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class Demo02Kafka {

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
        Properties kafkaSinkProps = new Properties();
        kafkaSinkProps.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaProducer<String> sinkFunc = new FlinkKafkaProducer<>("sink_topic", new SimpleStringSchema(), kafkaSinkProps);
        // job
        env.addSource(sourceFunc)
                .addSink(sinkFunc);
        env.execute("kafka-demo");

    }
}
