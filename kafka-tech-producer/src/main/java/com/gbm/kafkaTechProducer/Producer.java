package com.gbm.kafkaTechProducer;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Hello world!
 *
 */
public class Producer 
{
	static final String inputTopic = "price-input";
	static final String[] users = {"user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "user9", "user10"};
	public static KafkaProducer<String, Double> producer = null;
	
    public static void main( String[] args ) throws InterruptedException
    {
    	final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    	final Properties streamConfiguration = getStreamConfiguration(bootstrapServers);
    	producer = new KafkaProducer<String, Double>(streamConfiguration);
    	
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting Down");
                if (producer != null)
                    producer.close();
            }
        });
    	
    	Random random = new Random();
    	int log = 0;
    	while (true) {
    		log++;
    		for (String user : users) {
    			double price = 1000;
    			// Just a variation beetween 5 pesos
    			price = price + random.nextInt(5) - random.nextInt(5);
    	    	ProducerRecord<String, Double> record = new ProducerRecord<String, Double>(inputTopic, user, price);
    	    	
    	    	producer.send(record, new Callback() {
    				public void onCompletion(RecordMetadata metadata, Exception exception) {
    					if (exception != null) {
    						System.out.println("There was an error sending prices");
    						exception.printStackTrace();
    					}
    				}
    			});
    		}
    		
    		System.out.println("Ciclo: " + log);
    		Thread.sleep(5000); // Every 5000 ms is generating new info
    	}
    }
    
	private static Properties getStreamConfiguration(String bootstrapServers) {
		final Properties streamConfiguration = new Properties();
		streamConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		streamConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		streamConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class);
		
		return streamConfiguration;
	}
}
