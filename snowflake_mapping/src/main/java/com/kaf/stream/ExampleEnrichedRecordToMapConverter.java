package com.kaf.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.util.HashMap;
import java.util.Map;

public class ExampleEnrichedRecordToMapConverter implements MapFunction<com.kaf.stream.ExampleEnrichedRecord, Map<String, Object>> {
    @Override
    public Map<String, Object> map(com.kaf.stream.ExampleEnrichedRecord record) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", record.name);
        map.put("planet", record.planet);
        map.put("galaxy", record.galaxy);
        map.put("flinked_at", record.flinked_at);

        if (record.ase_internal != null) {
            map.put("ase_internal.valid", record.ase_internal.valid);
            map.put("ase_internal.errors", record.ase_internal.errors);
            map.put("ase_internal.schemaName", record.ase_internal.schemaName);
            map.put("ase_internal.schemaVersion", record.ase_internal.schemaVersion);
            map.put("ase_internal.timestamp", record.ase_internal.timestamp);
        }

        return map;
    }
}
