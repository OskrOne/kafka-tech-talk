package com.gbm.kafkaTechProducer.serde;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerializer<T> implements Serializer<T> {
	
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
	}

	public byte[] serialize(String topic, T data) {
		byte[] retVal = null;
		ObjectMapper mapper = new ObjectMapper();
		try {
			retVal = mapper.writeValueAsBytes(data);
		} catch (Exception ex) {
			System.out.println("There was an error serializating data");
			ex.printStackTrace();
		}
		return retVal;
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}
}
