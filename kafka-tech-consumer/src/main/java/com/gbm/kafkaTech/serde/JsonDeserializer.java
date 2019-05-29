package com.gbm.kafkaTech.serde;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonDeserializer<T> implements Deserializer<T> {
	
	private Class <T> type;
	
	public JsonDeserializer() {
	}

    public JsonDeserializer(Class type) {
        this.type = type;
    }
	
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		ObjectMapper mapper = new ObjectMapper();
		T result = null;
		try {
			result = mapper.readValue(data, type);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("There was an error deserializating bytes: " + data);
			e.printStackTrace();
		}
		
		return result;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
