package com.gachokaerick.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    private static final String bootstrapServer = "127.0.0.1:9092";
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            String key = "Id_" + i;
            String value = "hello world " + i;

            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", key, value);

            // send some data (asynchronous)
            producer.send(producerRecord, (recordMetadata, exception) -> {
                if (exception == null) {
                    logger.info("Received new metadata: \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Key: " + key + "\n" +
                            "Value: " + value + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() + "\n");
                } else {
                    logger.error("Error sending message" + exception.getMessage());
                }
            }).get(); // make call synchronous. don't do this in production!
        }

        producer.flush();
        producer.close();
    }
}
