package org.xmu.houseprice;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ElevatorNumProcessWindowFunction extends ProcessWindowFunction<Tuple3<Integer, Double, Integer>, Tuple3<Integer, Double, Integer>, Integer, TimeWindow> {
    @Override
    public void process(Integer key, Context context, Iterable<Tuple3<Integer, Double, Integer>> elements, Collector<Tuple3<Integer, Double, Integer>> out) {
        Tuple3<Integer, Double, Integer> result = elements.iterator().next();
        if (result.f2 == 0) {
            out.collect(Tuple3.of(key, 0.0, 0));
        } else {
            double avgHouseElevatorDivide = result.f1 / result.f2;
            out.collect(Tuple3.of(key, avgHouseElevatorDivide, result.f2));
        }
    }
}

