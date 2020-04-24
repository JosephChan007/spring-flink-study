package com.joseph.flink.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class WordCountTable {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> inputStream = env.socketTextStream("hdfs-host3", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordStream = inputStream.map(s -> {
            String[] fields = s.split(" ");
            return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        /*  SQL API
        tableEnv.registerDataStream("word_count", wordStream, "word, cnt");
        String sql = "select word, cnt.sum as counts from word_count group by word";
        Table table = tableEnv.sqlQuery(sql);
        DataStream<Tuple2<Boolean, Row>> wordCountStream = tableEnv.toRetractStream(table, Row.class);
        */

        /**
         * Table API
         */
        Table table = tableEnv.fromDataStream(wordStream, "word, cnt").groupBy("word").select("word, sum(cnt) as counts");
        DataStream<Tuple2<Boolean, Row>> wordCountStream = tableEnv.toRetractStream(table, Row.class);

        wordCountStream.filter(t -> t.f0).print();
        env.execute("商品总数");
    }

}
