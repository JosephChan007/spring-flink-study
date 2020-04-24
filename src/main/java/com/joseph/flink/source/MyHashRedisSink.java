package com.joseph.flink.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class MyHashRedisSink extends RichSinkFunction<Tuple3<String, String, String>> {

    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameter = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String host = parameter.getRequired("redis.host");
        String password = parameter.getRequired("redis.password");
        Integer port = parameter.getInt("redis.port", 6379);
        Integer database = parameter.getInt("redis.database", 0);

        jedis = new Jedis(host, port, 5000);
        jedis.auth(password);
        jedis.select(database);
    }

    @Override
    public void invoke(Tuple3<String, String, String> value, Context context) throws Exception {
        if(!jedis.isConnected()) jedis.connect();
        jedis.hset("ACTIVITY_UID", value.f0 + "_" + value.f1, value.f2);
    }

    @Override
    public void close() throws Exception {
        super.close();
        jedis.close();
    }

}
