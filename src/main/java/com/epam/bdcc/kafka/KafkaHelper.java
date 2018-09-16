package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;

import java.util.Arrays;
import java.util.Collection;

import static com.epam.bdcc.utils.PropertiesLoader.getKafkaConsumerProperties;
import static com.epam.bdcc.utils.PropertiesLoader.getKafkaProducerProperties;

public class KafkaHelper {

    public static KafkaProducer<String, MonitoringRecord> createProducer() {
        return new KafkaProducer<>(getKafkaProducerProperties());
    }

    public static ConsumerStrategy<String, MonitoringRecord> createConsumerStrategy(String topics) {
        Collection<String> topicsList = Arrays.asList(topics.split(","));
        return ConsumerStrategies.Subscribe(topicsList, getKafkaConsumerProperties());
    }

    public static String getKey(MonitoringRecord record) {
        return record.getStateCode() + "-" + record.getCountyCode() + "-" + record.getSiteNum() + "-"
                + record.getParameterCode() + "-" + record.getPoc();
    }
}
