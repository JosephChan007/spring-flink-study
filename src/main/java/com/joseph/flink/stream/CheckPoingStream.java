package com.joseph.flink.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class CheckPoingStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<String> stream = env.socketTextStream(args[0], 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                if("000".equalsIgnoreCase(s)) throw new Exception();
                return Tuple2.of(s, 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);

        sum.print();

        env.execute("分词统计");
    }

}
