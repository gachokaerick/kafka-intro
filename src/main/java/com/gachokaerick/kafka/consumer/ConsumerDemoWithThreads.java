package com.gachokaerick.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads() {
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getSimpleName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "mine";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating consumer thread...");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers, groupId, topic, latch
        );
        Thread thread = new Thread(myConsumerRunnable);
        thread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got error", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getSimpleName());
        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;
            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            consumer = new KafkaConsumer<>(properties);

            // subscribe consumer to topic
            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            try {
                // poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record :
                            records) {
                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info("Partition: " + record.partition() + " Offset: " + record.offset() + "\n");
                    }
                }
            } catch (WakeupException exception) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                // tell our main code we are done with our consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // interrupt consumer.poll() and throw WakeUpException
            consumer.wakeup();
        }
    }
}
