package com.epam.bdcc.spark;

import com.epam.bdcc.htm.HTMNetwork;
import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.htm.ResultState;
import com.epam.bdcc.kafka.KafkaHelper;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import scala.Tuple2;

import java.util.*;

public class AnomalyDetector implements GlobalConstants {

    public static void main(String[] args) throws Exception {
        //load a properties file from class path, inside static method
        final Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final String appName = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
            final String rawTopicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            final String enrichedTopicName = applicationProperties.getProperty(KAFKA_ENRICHED_TOPIC_CONFIG);
            final String checkpointDir = applicationProperties.getProperty(SPARK_CHECKPOINT_DIR_CONFIG);
            final Duration batchDuration = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG)));
            final Duration checkpointInterval = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_CHECKPOINT_INTERVAL_CONFIG)));

            SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[4]");
            JavaSparkContext sc = new JavaSparkContext(conf);
            JavaStreamingContext jssc = new JavaStreamingContext(sc, batchDuration);

            JavaDStream<MonitoringRecord> stream =
                KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    KafkaHelper.createConsumerStrategy(rawTopicName)
                ).map(ConsumerRecord::value);

            JavaPairDStream<String, MonitoringRecord> pairDStream = stream.mapToPair(
                record -> new Tuple2<>(KafkaHelper.getKey(record), record)
            );
            JavaMapWithStateDStream<String, MonitoringRecord, HTMNetwork, MonitoringRecord> stateDStream = pairDStream.mapWithState(StateSpec.function(mappingFunc));

            stateDStream.foreachRDD(rdd -> rdd.foreachPartition(partitionOfRecords -> {
                Producer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
                while (partitionOfRecords.hasNext()) {
                    MonitoringRecord record = partitionOfRecords.next();
                    System.out.println(producer.send(new ProducerRecord<>(enrichedTopicName, KafkaHelper.getKey(record), record)).get());
                }
                producer.close();
            }));

            jssc.checkpoint(checkpointDir);
            stream.checkpoint(checkpointInterval);

            jssc.start();
            jssc.awaitTermination();
        }
    }

    private static Function3<String, Optional<MonitoringRecord>, State<HTMNetwork>, MonitoringRecord> mappingFunc =
        (deviceID, recordOpt, state) -> {
            // case 0: timeout
            if (!recordOpt.isPresent())
                return null;

            // either new or existing device
            if (!state.exists())
                state.update(new HTMNetwork(deviceID));
            HTMNetwork htmNetwork = state.get();
            String stateDeviceID = htmNetwork.getId();
            if (!stateDeviceID.equals(deviceID))
                throw new Exception("Wrong behaviour of Spark: stream key is $deviceID%s, while the actual state key is $stateDeviceID%s");
            MonitoringRecord record = recordOpt.get();

            // get the value of DT and Measurement and pass it to the HTM
            HashMap<String, Object> m = new java.util.HashMap<>();
            m.put("DT", DateTime.parse(record.getDateGMT() + " " + record.getTimeGMT(), DateTimeFormat.forPattern("YY-MM-dd HH:mm")));
            m.put("Measurement", Double.parseDouble(record.getSampleMeasurement()));
            ResultState rs = htmNetwork.compute(m);
            record.setPrediction(rs.getPrediction());
            record.setError(rs.getError());
            record.setAnomaly(rs.getAnomaly());
            record.setPredictionNext(rs.getPredictionNext());

            return record;
        };
}