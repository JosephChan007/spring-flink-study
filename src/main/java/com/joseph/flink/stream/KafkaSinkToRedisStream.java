package com.joseph.flink.stream;

import com.joseph.flink.bean.FlinkEnvSouce;
import com.joseph.flink.source.DataStreamFactory;
import com.joseph.flink.source.MyRedisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class KafkaSinkToRedisStream {

    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromPropertiesFile(args[0]);
        FlinkEnvSouce flinkEnvSouce = DataStreamFactory.getKafkaSourceStream(parameter, SimpleStringSchema.class);
        flinkEnvSouce.getEnvironment().getConfig().setGlobalJobParameters(parameter);

        DataStream<String> stream = flinkEnvSouce.getStream();

        SingleOutputStreamOperator<Tuple2<String, Integer>> tumples = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                Arrays.stream(in.split(" ")).forEach(s -> out.collect(Tuple2.of(s, 1)));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = tumples.keyBy(0).sum(1);

        SingleOutputStreamOperator<Tuple3<String, String, String>> world_count = summed.map(new MapFunction<Tuple2<String, Integer>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(Tuple2<String, Integer> in) throws Exception {
                return Tuple3.of("world_count", in.f0, String.valueOf(in.f1));
            }
        });

        world_count.addSink(new MyRedisSink());

        flinkEnvSouce.getEnvironment().execute("分词统计");
    }

}
