package com.coloza.sample.kafka;

import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class ProducerAppTests {

	private ProducerApp app = new ProducerApp("localhost:9092");

	@Test
	public void produceMessage() {
		app.produceMessage("first_topic", "hello world");
	}

	@Test
	public void produceMessageWithCallback() {
		app.produceMessageWithCallback("first_topic", "hello world and callback");
	}

	@Test
	public void produceMessageKey() throws ExecutionException, InterruptedException {
		app.produceMessageKey("first_topic", "hello world and key");
	}

	@Test
	public void produceMessageWithSafeProducer() {
		app.produceMessageWithSafeProducer("first_topic", "hello world, i'm safe");
	}

	@Test
	public void produceMessageWithHighThroughputProducer() {
		app.produceMessageWithHighThroughputProducer("first_topic", "hello world, i'm fast");
	}
}
