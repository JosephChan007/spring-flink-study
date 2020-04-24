package com.joseph.flink.stream;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class CountWindowStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.socketTextStream("hdfs-host3", 8888);

        SingleOutputStreamOperator<Integer> nums = stream.map(s -> {
            try {
                return Integer.parseInt(s);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return 0;
        });
        AllWindowedStream<Integer, GlobalWindow> window = nums.countWindowAll(5);
        SingleOutputStreamOperator<Integer> sum = window.sum(0);
        sum.print();

        env.execute("分词统计");
    }

}
