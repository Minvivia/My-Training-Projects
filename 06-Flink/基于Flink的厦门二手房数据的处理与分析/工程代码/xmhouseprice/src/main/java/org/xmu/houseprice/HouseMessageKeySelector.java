package org.xmu.houseprice;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class HouseMessageKeySelector implements KeySelector<HouseMessage, Tuple2<String, Boolean>> {
    @Override
    public Tuple2<String, Boolean> getKey(HouseMessage houseMessage) {
        return Tuple2.of(houseMessage.getLocationInfo(), houseMessage.getElevator() > 0);
    }
}
