package com.kafkaconsumer.service;

import javax.jms.Queue;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

	@Autowired
	private JmsTemplate jmsTemplate;

	@Autowired
	private Queue queue;

	// method to consume kafka message and put into ActiveMQ
	@KafkaListener(topics = "test", groupId = "group_test")
	public void consume(String message) {
		System.out.println("Consumed message from Kafa Consumer : " + message);
		sendLogToActiveMQ(message);

	}

	// method for send log to ActiveMQ
	public void sendLogToActiveMQ(String message) {
		String filterMessage = null;
		try {
			JSONObject jsonObject = new JSONObject(message);
			filterMessage = jsonObject.get("message").toString();
			jmsTemplate.convertAndSend(queue, filterMessage);
			System.out.println("Successfully send message to ActiveMQ : " + filterMessage);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}
