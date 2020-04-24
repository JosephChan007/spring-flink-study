package com.joseph.flink.bean;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkEnvSouce<T> {

    private StreamExecutionEnvironment environment;
    private DataStream<T> stream;

    public FlinkEnvSouce() {
    }

    public FlinkEnvSouce(StreamExecutionEnvironment environment, DataStream<T> stream) {
        this.environment = environment;
        this.stream = stream;
    }


    public StreamExecutionEnvironment getEnvironment() {
        return environment;
    }

    public void setEnvironment(StreamExecutionEnvironment environment) {
        this.environment = environment;
    }

    public DataStream<T> getStream() {
        return stream;
    }

    public void setStream(DataStream<T> stream) {
        this.stream = stream;
    }

    public static<T> FlinkEnvSouce of(StreamExecutionEnvironment environment, DataStream<T> stream) {
        return new FlinkEnvSouce(environment, stream);
    }
}
