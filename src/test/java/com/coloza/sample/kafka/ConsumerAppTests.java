package com.coloza.sample.kafka;

import org.junit.Test;

import java.util.Arrays;

public class ConsumerAppTests {

	private ConsumerApp app = new ConsumerApp("localhost:9092", "my-first-app");

	@Test
	public void consumeMessage() {
		app.consumeMessage(Arrays.asList("first_topic"));
	}

	@Test
	public void consumeMessageWithThread() {
		app.consumeMessageWithThread(Arrays.asList("first_topic"));
	}
}
