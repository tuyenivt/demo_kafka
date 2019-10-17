package com.coloza.sample.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

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

}
