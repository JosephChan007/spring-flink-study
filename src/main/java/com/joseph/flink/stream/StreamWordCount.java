package com.joseph.flink.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.socketTextStream("hdfs-host3", 8888);

/*
        SingleOutputStreamOperator<String> words = stream.flatMap((String line, Collector<String> out) -> Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = words.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        DataStream<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);
*/

/*

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = stream.flatMap(
                (String line, Collector<Tuple2<String, Integer>> out) -> Arrays.stream(line.split(" "))
                .forEach(w -> out.collect(Tuple2.of(w, 1)))).returns(Types.TUPLE(Types.STRING, Types.INT)
        );
        DataStream<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);
*/

/*
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(
                (String line, Collector<Tuple2<String, Integer>> out) ->
                        Arrays.stream(line.split(" ")).forEach(w -> out.collect(Tuple2.of(w, 1)))
        ).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).sum(1);
*/

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(
                (String s, Collector<Tuple2<String, Integer>> out) -> Arrays.stream(s.split(" ")).map(w -> Tuple2.of(w, 1)).forEach(out::collect)
        ).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).sum(1);

        sum.print();
        env.execute("分词统计");
    }

}
