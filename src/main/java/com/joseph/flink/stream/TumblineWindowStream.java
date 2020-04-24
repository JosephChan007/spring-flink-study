package com.joseph.flink.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TumblineWindowStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.socketTextStream("hdfs-host3", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> nums = stream.map(s -> {
            String[] t = s.split(",");
            return Tuple2.of(t[0], Integer.parseInt(t[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = nums.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);

        sum.print();

        env.execute("分词统计");
    }

}
