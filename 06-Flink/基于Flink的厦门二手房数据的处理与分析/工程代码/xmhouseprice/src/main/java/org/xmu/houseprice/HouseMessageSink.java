package org.xmu.houseprice;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class HouseMessageSink implements SinkFunction<HouseMessage> {
    private int count = 0; // 计数器
    @Override
    public void invoke(HouseMessage value, Context context) throws Exception {
        count++; // 每次调用 invoke 方法时递增计数器
        System.out.println("Received a HouseMessage: " + value.toString());
        System.out.println("Total count so far: " + count);
    }
}