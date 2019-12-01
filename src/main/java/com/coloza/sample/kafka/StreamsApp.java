package com.coloza.sample.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsApp {

    private String bootstrapServer;
    private String applicationId;

    public StreamsApp(String bootstrapServer, String applicationId) {
        this.bootstrapServer = bootstrapServer;
        this.applicationId = applicationId;
    }

    private Properties createStreamsProperties() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, this.applicationId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        return properties;
    }

    public void stream(String fromTopic, String toTopic) {
        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream(fromTopic);
        KStream<String, String> filterStream = inputTopic.filter((k, v) -> v.length() > 5);
        filterStream.to(toTopic);

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), this.createStreamsProperties());

        // start our streams
        kafkaStreams.start();
    }
}
