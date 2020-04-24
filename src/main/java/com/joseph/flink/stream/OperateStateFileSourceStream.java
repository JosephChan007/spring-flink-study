package com.joseph.flink.stream;

import com.joseph.flink.bean.FlinkEnvSouce;
import com.joseph.flink.constant.EmDataStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class OperateStateFileSourceStream {

    public static void main(String[] args) throws Exception {

        FlinkEnvSouce flinkEnvSouce = EmDataStream.FILE.getSouce();
        DataStream<String> stream = flinkEnvSouce.getStream();

        flinkEnvSouce.getEnvironment().socketTextStream("hdfs-host3", 8888).map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                if ("000".equalsIgnoreCase(s)) {
                    throw new Exception();
                }
                return s;
            }
        }).print();

        stream.print();
        flinkEnvSouce.getEnvironment().execute("分词统计");
    }

}
