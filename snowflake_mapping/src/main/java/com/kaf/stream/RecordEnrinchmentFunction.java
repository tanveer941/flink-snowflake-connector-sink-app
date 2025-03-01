package com.kaf.stream;
import org.apache.flink.api.common.functions.MapFunction;

public class RecordEnrinchmentFunction implements MapFunction<ExampleRecord, ExampleEnrichedRecord> {
    @Override
    public ExampleEnrichedRecord map(ExampleRecord record) throws Exception {
        ExampleEnrichedRecord eer = new ExampleEnrichedRecord();
        eer.name = record.name;
        eer.planet = record.planet;
        eer.galaxy = record.galaxy;
        eer.flinked_at = System.currentTimeMillis();
        return eer;
    }
}
