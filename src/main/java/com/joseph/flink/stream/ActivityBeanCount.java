package com.joseph.flink.stream;

import com.joseph.flink.bean.ActivityBean;
import com.joseph.flink.bean.FlinkEnvSouce;
import com.joseph.flink.source.DataStreamFactory;
import com.joseph.flink.source.MyHashRedisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;


public class ActivityBeanCount {

    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromPropertiesFile(args[0]);
        FlinkEnvSouce<String> flinkEnvSouce = DataStreamFactory.getKafkaSourceStream(parameter, SimpleStringSchema.class);
        flinkEnvSouce.getEnvironment().getConfig().setGlobalJobParameters(parameter);
        flinkEnvSouce.getEnvironment().setParallelism(1);

        SingleOutputStreamOperator<ActivityBean> beanStream = flinkEnvSouce.getStream().map(new MapFunction<String, ActivityBean>() {
            @Override
            public ActivityBean map(String input) throws Exception {
                String[] fields = input.split(",");
                String uid = fields[0];
                String aid = fields[1];
                String date = fields[2];
                Integer type = Integer.valueOf(fields[3]);
                String province = fields[4];
                return ActivityBean.of(uid, aid, date, type, province);
            }
        });

        KeyedStream<ActivityBean, Tuple> keyedStream = beanStream.keyBy("aid", "type");

        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> sumedStream = keyedStream.map(new RichMapFunction<ActivityBean, Tuple3<String, Integer, Long>>() {

            private transient ValueState<BloomFilter> bloomFilterState;
            private transient ValueState<Long> countState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<BloomFilter> bloomFilterDescriptor = new ValueStateDescriptor<>("uid-bloomfilter-state", BloomFilter.class);
                bloomFilterState = getRuntimeContext().getState(bloomFilterDescriptor);

                ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<Long>("uid-count-state", Long.class);
                countState = getRuntimeContext().getState(countDescriptor);
            }

            @Override
            public Tuple3<String, Integer, Long> map(ActivityBean bean) throws Exception {
                String uid = bean.getUid();

                BloomFilter<String> bloomFilter = bloomFilterState.value();
                if (null == bloomFilter) {
                    bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                    bloomFilterState.update(bloomFilter);
                }

                Long count = (null == countState.value()) ? 0L : countState.value();

                if (!bloomFilter.mightContain(uid)) {
                    bloomFilter.put(uid);
                    bloomFilterState.update(bloomFilter);

                    countState.update(++count);
                }

                return Tuple3.of(bean.getAid(), bean.getType(), count);
            }
        });

        sumedStream.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(Tuple3<String, Integer, Long> input) throws Exception {
                return Tuple3.of(input.f0, String.valueOf(input.f1), String.valueOf(input.f2));
            }
        }).addSink(new MyHashRedisSink());

        flinkEnvSouce.getEnvironment().execute("活动人数统计");
    }


}
