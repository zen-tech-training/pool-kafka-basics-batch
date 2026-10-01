package com.zensar;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class CustomKafkaConsumer {

	public static void main(String[] args) {
		Properties props = getKafkaProperties();

		Consumer<String, String> consumer = new KafkaConsumer<>(props);
		System.out.println("Kafka consumer is ready!!");
		
		consumer.subscribe(Arrays.asList("ZenTopic"));
		Duration duration = Duration.ofMillis(100);
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(duration);
			for(ConsumerRecord<String, String> record: records) {
				System.out.println("Consumed - " + record.value());
			}
		}
		
	}

	public static Properties getKafkaProperties() {
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("group.id", "test-consumer-group");
		props.put("acks", "all");
		return props;
	}

}
