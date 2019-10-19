package com.coloza.sample.kafka;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ElasticSearchConsumerApp {

    private Logger log = LoggerFactory.getLogger(ElasticSearchConsumerApp.class);

    private final String HOSTNAME;
    private final String USERNAME;
    private final String PASSWORD;
    private final String BOOTSTRAP_SERVER;
    private final String GROUP_ID;

    public ElasticSearchConsumerApp(String hostname,
                                    String username,
                                    String password,
                                    String bootstrapServer,
                                    String groupId) {
        this.HOSTNAME = hostname;
        this.USERNAME = username;
        this.PASSWORD = password;
        this.BOOTSTRAP_SERVER = bootstrapServer;
        this.GROUP_ID = groupId;
    }

    private RestHighLevelClient createClient() {
        // don't do if you run a local ElasticSearch
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USERNAME, PASSWORD));

        RestClientBuilder builder = RestClient.builder(new HttpHost(HOSTNAME, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        return new RestHighLevelClient(builder);
    }

    public void index(String index, String type, String sourceJson) throws IOException {
        RestHighLevelClient client = this.createClient();
        IndexRequest indexRequest = new IndexRequest(index, type).source(sourceJson, XContentType.JSON);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        log.info("indexed an message with id " + id);
        client.close();
    }

    private Properties createConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.name().toLowerCase());
        return properties;
    }

    public void consumeMessageAndInsertToElasticSearch(List<String> topics, String index, String type) throws IOException {
        RestHighLevelClient client = this.createClient();
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.createConsumerProperties());

        // subscribe consumer to our topic(s)
        consumer.subscribe(topics);

        // poll new data and insert into Elastic Search
        for (int i = 0; i < 10; i++) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                IndexRequest indexRequest = new IndexRequest(index, type).source(record.value(), XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                log.info("indexed an message with id " + id);
                try {
                    TimeUnit.SECONDS.sleep(1L); // introduce a small delay, don't do this for production
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
