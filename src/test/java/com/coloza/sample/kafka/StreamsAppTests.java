package com.coloza.sample.kafka;

import org.junit.Test;

public class StreamsAppTests {

    private String bootstrapServer = "localhost:9092";
    private String applicationId = "my-streams-app";
    private StreamsApp app = new StreamsApp(bootstrapServer, applicationId);

    @Test
    public void streamTest() {
        app.stream("first_topic", "second_topic");
    }
}
