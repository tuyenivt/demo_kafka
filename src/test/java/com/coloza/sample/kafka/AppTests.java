package com.coloza.sample.kafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

public class AppTests {

	private App app = new App("localhost:9092");

	@Test
	public void produceMessage() {
		app.produceMessage("first_topic", "hello world");
	}

	@Test
	public void produceMessageWithCallback() {
		app.produceMessageWithCallback("first_topic", "hello world and callback");
	}

}
