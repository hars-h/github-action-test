package com.azure.eventhub.producer;

import java.util.HashMap;
import java.util.Map;

//Copyright (c) Microsoft Corporation. All rights reserved.
//Licensed under the MIT License.
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;

public class DataReporter {

	private final String TOPIC;

	private Producer<String, String> producer;

	public DataReporter(final Producer<String, String> producer, String TOPIC) {
		this.producer = producer;
		this.TOPIC = TOPIC;
	}

	public void ksend(String data,HashMap<String, String> header) {
		//long time = System.currentTimeMillis();
		final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, data);
		createRecordHeader(record.headers(),header);
		producer.send(record, new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					System.out.println(exception);
					System.exit(1);
				}
			}
		});
	}

	private void createRecordHeader(Headers headers, HashMap<String, String> header) {
		
		for (Map.Entry<String,String> entry : header.entrySet()) { 
			
            headers.add(entry.getKey(),entry.getValue().getBytes());
		}
		
	}
	

}