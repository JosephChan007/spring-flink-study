package com.joseph.flink.constant;

import com.joseph.flink.bean.FlinkEnvSouce;
import com.joseph.flink.source.DataStreamFactory;

public enum EmDataStream {

    SOCKET(0, "socket-stream", DataStreamFactory.getSocketSourceStream()),
    KAFKA(1, "kafka-stream", DataStreamFactory.getKafkaSourceStream()),
    FILE(1, "kafka-stream", DataStreamFactory.getFileSourceStream()),

    ;

    private int code;
    private String name;
    private FlinkEnvSouce souce;

    EmDataStream(int code, String name, FlinkEnvSouce souce) {
        this.code = code;
        this.name = name;
        this.souce = souce;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FlinkEnvSouce getSouce() {
        return souce;
    }

    public void setSouce(FlinkEnvSouce souce) {
        this.souce = souce;
    }
}
