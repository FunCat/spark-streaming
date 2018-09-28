package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class TopicGenerator implements GlobalConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);

    public static void main(String[] args) {
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final boolean skipHeader = Boolean
                .parseBoolean(applicationProperties.getProperty(GENERATOR_SKIP_HEADER_CONFIG));
            final long batchSleep = Long.parseLong(applicationProperties.getProperty(GENERATOR_BATCH_SLEEP_CONFIG));
            final int batchSize = Integer.parseInt(applicationProperties.getProperty(BATCH_SIZE_CONFIG));
            final String sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);

            try (Stream<String> stream = Files.lines(Paths.get(sampleFile));
                 Producer<String, MonitoringRecord> producer = KafkaHelper.createProducer()) {
                stream.forEach(line -> {
                    try {
                        MonitoringRecord monitoringRecord = new MonitoringRecord(line.split(","));
                        RecordMetadata recordMetadata = sendMessage(topicName, producer, monitoringRecord);
                        LOGGER.info(recordMetadata.toString());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException e) {
                LOGGER.error("Fail to read the text file", e);
            }
        }
    }

    public static RecordMetadata sendMessage(String topicName, Producer<String, MonitoringRecord> producer, MonitoringRecord monitoringRecord) throws InterruptedException, ExecutionException {
        return producer.send(new ProducerRecord<>(topicName, KafkaHelper.getKey(monitoringRecord), monitoringRecord)).get();
    }
}
