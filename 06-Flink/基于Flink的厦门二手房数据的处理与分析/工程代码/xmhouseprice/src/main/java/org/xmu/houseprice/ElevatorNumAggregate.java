package org.xmu.houseprice;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.AggregateFunction;


public class ElevatorNumAggregate implements AggregateFunction<HouseMessage, Tuple3<Integer, Double, Integer>, Tuple3<Integer, Double, Integer>> {
    @Override
    public Tuple3<Integer, Double, Integer> createAccumulator() {
        return Tuple3.of(0, 0.0, 0);
    }

    @Override
    public Tuple3<Integer, Double, Integer> add(HouseMessage value, Tuple3<Integer, Double, Integer> accumulator) {
        return Tuple3.of(value.getBuildingYear(), accumulator.f1 + value.getElevatorNum(), accumulator.f2 + 1); // Sum elevatorNum and count records
    }

    @Override
    public Tuple3<Integer, Double, Integer> getResult(Tuple3<Integer, Double, Integer> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple3<Integer, Double, Integer> merge(Tuple3<Integer, Double, Integer> a, Tuple3<Integer, Double, Integer> b) {
        return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
    }
}


