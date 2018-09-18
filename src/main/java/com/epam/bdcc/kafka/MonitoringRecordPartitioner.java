package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MonitoringRecordPartitioner extends DefaultPartitioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringRecordPartitioner.class);

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (value instanceof MonitoringRecord) {
            return KafkaHelper.getKey((MonitoringRecord) value).hashCode() % 10;
        } else {
            return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
        }
    }

    public void close() {
        super.close();
        //TODO : Add implementation for close, if needed
        throw new UnsupportedOperationException("Add implementation for close");
    }

    public void configure(Map<String, ?> map) {
        //TODO : Add implementation for configure, if needed
//        throw new UnsupportedOperationException("Add implementation for configure");
    }
}