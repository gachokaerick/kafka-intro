package com.gachokaerick.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

// assign and seek are mostly used to replay data or fetch a specific message
public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getSimpleName());

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Collections.singletonList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, 15L);

        // poll for new data
        int messagesRead = 0;
        boolean keepReading = true;
        int messagesToRead = 5;
        while (keepReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record :
                    records) {
                logger.info("Key: " + record.key() + " Value: " + record.value());
                logger.info("Partition: " + record.partition() + " Offset: " + record.offset() + "\n");
                messagesRead++;
                if (messagesRead >= messagesToRead) {
                    keepReading = false;
                    break;
                }
            }
        }

        logger.info("exiting application");
    }
}
