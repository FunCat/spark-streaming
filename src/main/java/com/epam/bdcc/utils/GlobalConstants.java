package com.epam.bdcc.utils;

public interface GlobalConstants {
    //Global config
    String PROPERTIES_FILE = "conf/config.properties";

    //Generator config
    String GENERATOR_SKIP_HEADER_CONFIG = "skip.header";
    String GENERATOR_SAMPLE_FILE_CONFIG = "sample.file";
    String GENERATOR_BATCH_SLEEP_CONFIG = "batch.sleep";

    //Kafka config
    String KAFKA_RAW_TOPIC_CONFIG = "raw.topic";
    String KAFKA_ENRICHED_TOPIC_CONFIG = "enriched.topic";
    String BATCH_SIZE_CONFIG = "batch.size";

    //Spark config
    String SPARK_APP_NAME_CONFIG = "app.name";
    String SPARK_CHECKPOINT_DIR_CONFIG = "checkpoint.dir";
    String SPARK_BATCH_DURATION_CONFIG = "batch.duration";
    String SPARK_CHECKPOINT_INTERVAL_CONFIG = "checkpoint.interval";
    String SPARK_WINDOW_DURATION_CONFIG = "window.duration";
    String SPARK_INTERNAL_SERIALIZER_CONFIG = "spark.serializer";
    String SPARK_KRYO_REGISTRATOR_CONFIG = "spark.kryo.registrator";
    String SPARK_KRYO_REGISTRATOR_REQUIRED_CONFIG = "spark.kryo.registrationRequired";
}
