package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.serde.KafkaJsonMonitoringRecordSerDe;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;


public class TopicGeneratorTest {
    private MockProducer<String, MonitoringRecord> producer;
    private TopicGenerator generator;
    private static String TOPIC = "monitoring20";

    @Before
    public void setUp() {
        producer = new MockProducer<>(true, new StringSerializer(), new KafkaJsonMonitoringRecordSerDe());
        generator = new TopicGenerator();
    }

    @Test
    public void testProducer() throws ExecutionException, InterruptedException {

        String key = "10-001-0002-44201-1";
        MonitoringRecord record = new MonitoringRecord("10", "001", "0002", "44201", "1", "38.986672",
            "-75.5568", "WGS84", "Ozone", "2014-01-01", "00:00", "2014-01-01", "05:00", "0.016",
            "Parts per million", "0.005", "", "", "FEM", "047", "INSTRUMENTAL - ULTRA VIOLET", "Delaware",
            "Kent", "2014-02-12", 0.0, 0.0, 0.0, 0.0);
        String key2 = "11-001-0042-40511-10";
        MonitoringRecord record2 = new MonitoringRecord("11", "001", "0042", "40511", "10", "34.997672",
            "-75.5588", "REW26", "Ozone", "2014-01-01", "00:00", "2014-01-01", "05:00", "0.016",
            "Parts per million", "0.005", "", "", "FEM", "047", "INSTRUMENTAL - ULTRA VIOLET", "Delaware",
            "Kent", "2014-02-12", 0.0, 0.0, 0.0, 0.0);

        generator.sendMessage(TOPIC, producer, record);
        generator.sendMessage(TOPIC, producer, record2);

        List<ProducerRecord<String, MonitoringRecord>> history = producer.history();

        List<ProducerRecord<String, MonitoringRecord>> expected = Arrays.asList(
            new ProducerRecord<>(TOPIC, key, record),
            new ProducerRecord<>(TOPIC, key2, record2)
        );

        assertEquals(expected, history);
    }

    @Test
    public void testProducerWithFile() throws IOException {
        Stream<String> stream = Files.lines(Paths.get("data/one_device_2015-2017.csv"));

        stream.forEach(line -> {
            try {
                MonitoringRecord monitoringRecord = new MonitoringRecord(line.split(","));
                generator.sendMessage(TOPIC, producer, monitoringRecord);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        List<ProducerRecord<String, MonitoringRecord>> history = producer.history();
        assertEquals(25984, history.size());
    }

}