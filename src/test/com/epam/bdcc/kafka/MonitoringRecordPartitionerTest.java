package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MonitoringRecordPartitionerTest {

    private MonitoringRecordPartitioner partitioner = new MonitoringRecordPartitioner();

    @Test
    void partition() {
        String topic = "monitoring20";
        String key = "10-001-0002-44201-1";
        MonitoringRecord record = new MonitoringRecord("10", "001", "0002", "44201", "1", "38.986672",
            "-75.5568", "WGS84", "Ozone", "2014-01-01", "00:00", "2014-01-01", "05:00", "0.016",
            "Parts per million", "0.005", "", "", "FEM", "047", "INSTRUMENTAL - ULTRA VIOLET", "Delaware",
            "Kent", "2014-02-12", 0.0, 0.0, 0.0, 0.0);
        int expected = 0;

        int actual = partitioner.partition(topic, key, null, record, null, null);

        assertEquals(expected, actual);
    }
}