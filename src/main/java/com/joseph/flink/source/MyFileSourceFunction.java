package com.joseph.flink.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.concurrent.TimeUnit;

public class MyFileSourceFunction extends RichParallelSourceFunction<Tuple2<String, String>> implements CheckpointedFunction {

    private String path;
    private boolean flag = true;
    private long offset = 0L;
    private transient ListState<Long> offsetState;

    public MyFileSourceFunction(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        Iterable<Long> it = offsetState.get();
        if (it.iterator().hasNext()) {
            offset = it.iterator().next();
        }

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        RandomAccessFile accessFile = new RandomAccessFile(path + "/" + subtaskIndex + ".txt", "r");
        accessFile.seek(offset);

        Object lock = ctx.getCheckpointLock();

        while (flag) {
            String line = accessFile.readLine();
            if (StringUtils.isNotBlank(line)) {
                line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
                synchronized (lock) {
                    offset = accessFile.getFilePointer();
                    ctx.collect(Tuple2.of(subtaskIndex + "", line));
                }
            } else {
                TimeUnit.MILLISECONDS.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetState.clear();
        offsetState.add(offset);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("file-readline-offset", Long.class);
        offsetState = context.getOperatorStateStore().getListState(descriptor);
    }
}
