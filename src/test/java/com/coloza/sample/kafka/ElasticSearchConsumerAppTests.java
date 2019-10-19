package com.coloza.sample.kafka;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class ElasticSearchConsumerAppTests {

    private final String HOSTNAME = "kafka-test-509056955.ap-southeast-2.bonsaisearch.net";
    private final String USERNAME = "v7mvn1jrza";
    private final String PASSWORD = "jrdr4uauhn";
    private final String BOOTSTRAP_SERVER = "localhost:9092";
    private final String GROUP_ID = "my-elasticsearch-app";
    private ElasticSearchConsumerApp app = new ElasticSearchConsumerApp(HOSTNAME, USERNAME, PASSWORD, BOOTSTRAP_SERVER, GROUP_ID);

    @Test
    public void indexTest() throws IOException {
        app.index("twitter", "tweets", "{\"foo\": \"bar\"}");
    }

    @Test
    public void consumeMessageAndInsertToElasticSearchTest() throws IOException {
        app.consumeMessageAndInsertToElasticSearch(Arrays.asList("twitter_tweets"), "twitter", "tweets");
    }

    @Test
    public void consumeMessageAndInsertToElasticSearchUsingBatchTest() throws IOException {
        app.consumeMessageAndInsertToElasticSearchUsingBatch(Arrays.asList("twitter_tweets"), "twitter", "tweets");
    }
}
