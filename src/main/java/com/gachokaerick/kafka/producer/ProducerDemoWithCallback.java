package com.gachokaerick.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final String bootstrapServer = "127.0.0.1:9092";
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 200; i++) {
            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hello world " + i);

            // send some data (asynchronous)
            producer.send(producerRecord, (recordMetadata, exception) -> {
                if (exception == null) {
                    logger.info("Received new metadata: \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() + "\n");
                } else {
                    logger.error("Error sending message" + exception.getMessage());
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
