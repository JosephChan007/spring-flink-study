package com.joseph.flink.stream;

import com.joseph.flink.bean.FlinkEnvSouce;
import com.joseph.flink.constant.EmDataStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class KeyedStateStream {

    public static void main(String[] args) throws Exception {

        FlinkEnvSouce flinkEnvSouce = EmDataStream.KAFKA.getSouce();
        DataStream<String> stream = flinkEnvSouce.getStream();

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordMap = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordMap.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<Integer> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("word-count-state", Integer.class);
                state = getRuntimeContext().getState(descriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
                Integer result = (null == state.value()) ? input.f1 : input.f1 + state.value();
                state.update(result);
                return Tuple2.of(input.f0, result);
            }
        });

        summed.print();

        flinkEnvSouce.getEnvironment().execute("分词统计");
    }

}
