package com.coloza.sample.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class App {

    private String bootstrapServer;
    private Logger log = LoggerFactory.getLogger(App.class);

    public App(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    private Properties createProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public void produceMessage(String topic, String message) {
        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(this.createProducerProperties());

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

        // send data - asynchronous (data is not send because app shutdown immediately)
        producer.send(record);

        producer.flush();
        producer.close();
    }

    public void produceMessageWithCallback(String topic, String message) {
        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(this.createProducerProperties());

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

        // send data - asynchronous (data is not send because app shutdown immediately)
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // execute every time a record is successful sent or an exception is thrown
                if (e == null) {
                    // the record was successful sent
                    log.info("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    log.error("Error while producing", e);
                }
            }
        });

        producer.flush();
        producer.close();
    }

}
