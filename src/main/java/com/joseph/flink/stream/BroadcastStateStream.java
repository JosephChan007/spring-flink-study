package com.joseph.flink.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.joseph.flink.bean.ActivityBean;
import com.joseph.flink.bean.FlinkEnvSouce;
import com.joseph.flink.source.DataStreamFactory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class BroadcastStateStream {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromPropertiesFile(args[0]);
        FlinkEnvSouce flinkEnvSouce = DataStreamFactory.getKafkaSourceStream(
                parameter, "flink-broadcast",
                UUID.randomUUID().toString(), SimpleStringSchema.class);

        /**
         * 活动名称配置Source流
         */
        DataStream<String> kafkaConfigStream = flinkEnvSouce.getStream();
        SingleOutputStreamOperator<Tuple3<String, String, String>> kafkaConfigTupleStream = kafkaConfigStream.process(
            new ProcessFunction<String, Tuple3<String, String, String>>() {
                @Override
                public void processElement(String input, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                    JSONObject jsonObject = JSON.parseObject(input);
                    JSONArray jsonArray = jsonObject.getJSONArray("data");
                    String sqlType = jsonObject.getString("type");
                    if ("insert".equalsIgnoreCase(sqlType) || "update".equalsIgnoreCase(sqlType) || "delete".equalsIgnoreCase(sqlType)) {
                        for (int i = 0; i < jsonArray.size(); i++) {
                            JSONObject dataJson = jsonArray.getJSONObject(i);
                            String aid = dataJson.getString("a_id");
                            String aname = dataJson.getString("a_name");
                            out.collect(Tuple3.of(aid, aname, sqlType));
                        }
                    }
                }
            }
        );

        /**
         * 活动数据Source流
         */
        FlinkEnvSouce flinkEnvSouce1 = DataStreamFactory.getKafkaSourceStream(
                parameter, "flink-test1",
                "a001", SimpleStringSchema.class);
        DataStream<String> kafkaDataStream = flinkEnvSouce1.getStream();
        SingleOutputStreamOperator<ActivityBean> activityBeanStream = kafkaDataStream.map(input -> {
                String[] fields = input.split(",");
                String uid = fields[0];
                String aid = fields[1];
                String time = fields[2];
                Integer type = Integer.valueOf(fields[3]);
                String province = fields[4];
                return ActivityBean.of(uid, aid, time, type, province);
        });

        MapStateDescriptor<String, String> broadcastDescriptor = new MapStateDescriptor<>("activity-name-state1", String.class, String.class);
        BroadcastStream<Tuple3<String, String, String>> broadcastStateStream = kafkaConfigTupleStream.broadcast(broadcastDescriptor);

        SingleOutputStreamOperator<Tuple4<String, String, String, String>> broadcastResultStream = activityBeanStream.connect(broadcastStateStream).process(
            new BroadcastProcessFunction<ActivityBean, Tuple3<String, String, String>, Tuple4<String, String, String, String>>() {

                /**
                 * 活动数据流处理
                 */
                @Override
                public void processElement(ActivityBean bean, ReadOnlyContext ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {
                    ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastDescriptor);
                    String aname = broadcastState.get(bean.getAid());
                    out.collect(Tuple4.of(bean.getUid(), aname, bean.getTime(), bean.getProvince()));
                }

                /**
                 * 配置数据流处理
                 */
                @Override
                public void processBroadcastElement(Tuple3<String, String, String> input, Context ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {
                    String aid = input.f0;
                    String aname = input.f1;
                    String sqlType = input.f2;
                    BroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastDescriptor);
                    if ("delete".equalsIgnoreCase(sqlType)) {
                        broadcastState.remove(aid);
                    } else {
                        broadcastState.put(aid, aname);
                    }

                    // broadcastState.entries().forEach(e -> System.out.println("key:" + e.getKey() + ", value:" + e.getValue()));
                }
            }
        );

        broadcastResultStream.print();
        flinkEnvSouce.getEnvironment().execute("广播变量流处理");
    }



}
