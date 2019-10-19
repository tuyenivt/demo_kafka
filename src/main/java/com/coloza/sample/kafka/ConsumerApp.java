package com.coloza.sample.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerApp {

    private Logger log = LoggerFactory.getLogger(ConsumerApp.class);
    private String bootstrapServer;
    private String groupId;

    public ConsumerApp(String bootstrapServer, String groupId) {
        this.bootstrapServer = bootstrapServer;
        this.groupId = groupId;
    }

    private Properties createConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        return properties;
    }

    public void consumeMessage(List<String> topics) {
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.createConsumerProperties());

        // subscribe consumer to our topic(s)
        consumer.subscribe(topics);

        // poll new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value(),
                        ", Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }

    public void consumeMessageWithThread(List<String> topics) {
        // latch for dealing with multiple thread
        CountDownLatch latch = new CountDownLatch(1);

        // create consumer runnable
        log.info("Creating the consumer thread");
        Runnable consumerRunnable = new ConsumerRunnable(
                this.bootstrapServer,
                this.createConsumerProperties(),
                this.groupId,
                topics,
                latch);

        // start the thread
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has exited");
        }));

        // add a shutdown hook

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrupted: ", e);
        } finally {
            log.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private Logger log = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private String bootstrapServer;
        private String groupId;

        public ConsumerRunnable(String bootstrapServer,
                                Properties properties,
                                String groupId,
                                List<String> topics,
                                CountDownLatch latch) {
            this.latch = latch;
            this.bootstrapServer = bootstrapServer;
            this.groupId = groupId;
            // create consumer
            consumer = new KafkaConsumer<>(properties);
            // subscribe consumer to our topic(s)
            consumer.subscribe(topics);
        }

        @Override
        public void run() {
            // poll new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Key: " + record.key() + ", Value: " + record.value(),
                                ", Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                log.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw exception WakeupException
            consumer.wakeup();
        }
    }

    public void consumeMessageAssignAndSeek(String topic, long offsetToReadFrom, int numberOfMessagesToRead) {
        Properties properties = this.createConsumerProperties();
        // this don't need the group_id
        properties.remove(ConsumerConfig.GROUP_ID_CONFIG);
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.createConsumerProperties());

        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        boolean isKeepReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll new data
        while (isKeepReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value(),
                        ", Partition: " + record.partition() + ", Offset: " + record.offset());
                numberOfMessagesReadSoFar++;
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    isKeepReading = false; // stop reading
                    break; // exist while loop
                }
            }
        }

        log.info("Exiting the application");
    }

}
