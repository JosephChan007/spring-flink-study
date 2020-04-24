package com.joseph.flink.source;

import com.joseph.flink.bean.FlinkEnvSouce;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class DataStreamFactory {

    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }

    public static FlinkEnvSouce getSocketSourceStream() {
        DataStreamSource<String> stream = env.socketTextStream("hdfs-host3", 8888);
        return FlinkEnvSouce.of(env, stream);
    }

    public static FlinkEnvSouce getFileSourceStream() {
        env.setParallelism(2);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        DataStreamSource<Tuple2<String, String>> stream = env.addSource(new MyFileSourceFunction("D://"));
        return FlinkEnvSouce.of(env, stream);
    }

    public static FlinkEnvSouce getKafkaSourceStream() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hdfs-host1:9092,hdfs-host2:9092,hdfs-host3:9092,hdfs-host4:9092");
        properties.setProperty("group.id", "flink0");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("enable.auto.commit", "false");

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("flink-test", new SimpleStringSchema(), properties);
        kafkaSource.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> stream = env.addSource(kafkaSource);
        return FlinkEnvSouce.of(env, stream);
    }

    public static <T> FlinkEnvSouce getKafkaSourceStream(ParameterTool parameter, Class<? extends DeserializationSchema<T>> clazz) throws Exception {
        String group = parameter.getRequired("kafka.group.id");
        String topics = parameter.get("kafka.topics", "flink-test");
        return getKafkaSourceStream(parameter, topics, group, clazz);
    }

    public static <T> FlinkEnvSouce getKafkaSourceStream(ParameterTool parameter, String topics, String group, Class<? extends DeserializationSchema<T>> clazz) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", parameter.getRequired("kafka.bootstrap.servers"));
        properties.setProperty("group.id", group);
        properties.setProperty("auto.offset.reset", parameter.get("kafka.auto.offset.reset", "earliest"));
        properties.setProperty("enable.auto.commit", parameter.get("kafka.enable.auto.commit", "false"));

        env.enableCheckpointing(parameter.getLong("flink.checkpoint.interval", 5000));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        List<String> topicList = Arrays.asList(topics.split(","));
        FlinkKafkaConsumer<T> kafkaSource = new FlinkKafkaConsumer<>(topicList, clazz.newInstance(), properties);
        kafkaSource.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<T> stream = env.addSource(kafkaSource);
        return FlinkEnvSouce.of(env, stream);
    }

}
