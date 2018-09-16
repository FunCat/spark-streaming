package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaJsonMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonMonitoringRecordSerDe.class);

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //TODO : Add implementation for configure, if needed
//        throw new UnsupportedOperationException("Add implementation for configure");
    }

    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {
        byte[] retVal = null;
        try {
            retVal = mapper.writeValueAsString(data).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public MonitoringRecord deserialize(String topic, byte[] data) {
        MonitoringRecord record = null;
        try {
            record = mapper.readValue(data, MonitoringRecord.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return record;
    }

    @Override
    public void close() {
        //TODO : Add implementation for close, if needed
//        throw new UnsupportedOperationException("Add implementation for close");
    }
}