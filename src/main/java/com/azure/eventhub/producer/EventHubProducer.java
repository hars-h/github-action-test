package com.azure.eventhub.producer;

//Copyright (c) Microsoft Corporation. All rights reserved.
//Licensed under the MIT License.
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Properties;
import java.io.FileReader;

public class EventHubProducer {

	/*
	 * public static void main(String[] args) { EventHubProducer ehp =new
	 * EventHubProducer(); try { ehp.produce("TEST",
	 * "t30f011232daea2f9.dp.downstream"); } catch (Exception e) { // TODO
	 * Auto-generated catch block e.printStackTrace(); } }
	 */
	public static Producer<String, String> producer = null;
	

	private static Producer<String, String> createProducer() {
		try {
			Properties properties = new Properties();
			properties.load(new FileReader("src/main/resources/producer.config"));
			properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
			properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
			properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			return new KafkaProducer<>(properties);
		} catch (Exception e) {
			System.out.println("Failed to create producer with exception: " + e);
			System.exit(0);
			return null;
		}
	}

	public void produce(String string, HashMap<String, String> result, String topicName) {
		// TODO Auto-generated method stub
		if(producer==null) {
		producer = createProducer();
		}
		
		DataReporter tdr = new DataReporter(producer, topicName);
		tdr.ksend(string,result);
	}

}
