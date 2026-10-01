package com.zensar;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class CustomKafkaProducer {

	public static void main(String[] args) {
		Properties props = getKafkaProperties();
		Producer<String, String> producer = new KafkaProducer<>(props);
		System.out.println("Kafka producer is ready!!");
		
		ProducerRecord<String, String> message_1 = new ProducerRecord<>("ZenTopic", "flipkart.shoes", "Please buy Adidas shoes again");
		ProducerRecord<String, String> message_2 = new ProducerRecord<>("ZenTopic", "flipkart.cloths", "Please buy T-Shirt for me again");
		
		producer.send(message_1);
		producer.send(message_2);
		
		producer.close();
		System.out.println("Messages produced..");
	}

	public static Properties getKafkaProperties() {
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "all");
		return props;
	}
}
