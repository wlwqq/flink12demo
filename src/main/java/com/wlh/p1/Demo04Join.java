package com.wlh.p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class Demo04Join {
    public static void main(String[] args) throws Exception {
        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 7777);
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 8888);
        KeyedStream<Tuple2<String, String>, String> keyedStream1 = stream1.map(new MyMap()).keyBy(new MyKeySelector());
        KeyedStream<Tuple2<String, String>, String> keyedStream2 = stream2.map(new MyMap()).keyBy(new MyKeySelector());

        keyedStream1.connect(keyedStream2)
                .process(new MyKeyedCoProcessFunc())
                .print();
        //
        env.execute();
    }

    static class MyMap implements MapFunction<String, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> map(String value) throws Exception {
            String[] split = value.split(",");
            return Tuple2.of(split[0], split[1]);
        }
    }

    static class MyKeySelector implements KeySelector<Tuple2<String, String>, String> {
        @Override
        public String getKey(Tuple2<String, String> value) throws Exception {
            return value.f0;
        }
    }

    static class MyKeyedCoProcessFunc extends KeyedCoProcessFunction<String, Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>> {

        private ValueState<Tuple2<String, String>> leftState;
        private ValueState<Long> leftStateTimer;
        private ValueState<Tuple2<String, String>> rightState;
        private ValueState<Long> rightStateTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 创建StateTtlConfig
//            StateTtlConfig leftStateTtlConfig = StateTtlConfig
//                    .newBuilder(Time.seconds(10)) // 设置TTL时间为10秒
//                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 更新TTL的时间点
//                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 过期状态的可见性
//                    .build();
            ValueStateDescriptor<Tuple2<String, String>> leftStateDesc = new ValueStateDescriptor<>(
                    "leftState",
                    TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                    }));
//            leftStateDesc.enableTimeToLive(leftStateTtlConfig);
            leftState = getRuntimeContext().getState(leftStateDesc);
            //
            rightState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(
                            "rightState",
                            TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                            })
                    )
            );

            // 定时器时间戳状态
            leftStateTimer = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(
                            "leftStateTimer",
                            Long.class
                    )
            );

            rightStateTimer = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(
                            "rightStateTimer",
                            Long.class
                    )
            );
        }

        @Override
        public void processElement1(Tuple2<String, String> value,
                                    KeyedCoProcessFunction<String, Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>>.Context ctx,
                                    Collector<Tuple3<String, String, String>> out) throws Exception {
            // 把数据放在状态
            leftState.update(value);
            Long oldTimer = leftStateTimer.value();
            if (oldTimer != null) {
                ctx.timerService().deleteProcessingTimeTimer(oldTimer);
            }
            long timer = ctx.timerService().currentProcessingTime() + 30 * 1000L;
            leftStateTimer.update(timer);
            ctx.timerService().registerProcessingTimeTimer(timer);

            // 查询右表是否有数据
            Tuple2<String, String> rightRecord = rightState.value();
            if (rightRecord != null) {
                // 右表有数据 join输出
                out.collect(Tuple3.of(value.f0, value.f1, rightRecord.f1));
            } else {
                // 右表没有数据 输出 left null
                out.collect(Tuple3.of(value.f0, value.f1, null));
            }
        }

        @Override
        public void processElement2(Tuple2<String, String> value,
                                    KeyedCoProcessFunction<String, Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>>.Context ctx,
                                    Collector<Tuple3<String, String, String>> out) throws Exception {
            //  把数据放在状态
            rightState.update(value);
            // 查询左表是否有数据
            Tuple2<String, String> leftRecord = leftState.value();
            if (leftRecord != null) {
                // 左表有数据 join输出
                out.collect(Tuple3.of(value.f0, leftRecord.f1, value.f1));
            } else {
                // 左表没有数据 输出 null right，
                out.collect(Tuple3.of(value.f0, null, value.f1));
            }

        }


        @Override
        public void onTimer(long timestamp,
                            KeyedCoProcessFunction<String, Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>>.OnTimerContext ctx,
                            Collector<Tuple3<String, String, String>> out) throws Exception {
            System.out.println("定时器触发");
            if (leftStateTimer.value() != null && leftStateTimer.value().longValue() == timestamp) {
                System.out.println("左流定时器触发");
                // 左流的定时器
                leftState.clear();
                leftStateTimer.clear();
            }

            if (rightStateTimer.value() != null && rightStateTimer.value().longValue() == timestamp) {
                System.out.println("右流定时器触发");
                // 右流的定时器
                rightState.clear();
                rightStateTimer.clear();
            }
        }
    }
}
