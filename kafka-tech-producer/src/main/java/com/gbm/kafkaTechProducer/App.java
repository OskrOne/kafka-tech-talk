package com.gbm.kafkaTechProducer;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.gbm.kafkaTechProducer.model.Trade;
import com.gbm.kafkaTechProducer.serde.JsonSerializer;
/**
 * Hello world!
 *
 */
public class App 
{
	static final String inputTopic = "stocks-input";
	static final String[] tickers = {"MMM", "ABT", "ABBV", "ACN", "ATVI", "AYI", "ADBE", "AAP", "AES", "AET"};
	public static KafkaProducer<String, Trade> producer = null;
	
    public static void main( String[] args ) throws InterruptedException
    {
    	final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    	final Properties streamConfiguration = getStreamConfiguration(bootstrapServers);
    	producer = new KafkaProducer<String, Trade>(streamConfiguration);
    	
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting Down");
                if (producer != null)
                    producer.close();
            }
        });
    	
    	Random random = new Random();
    	while (true) {
    		for (String ticker : tickers) {
    			int size = random.nextInt(100);
    			int price = 1000;
    			// Just a variation beetween 5 pesos
    			price = price + random.nextInt(5) - random.nextInt(5);
    			
    			Trade trade = new Trade("ASK", ticker, price, size);
    	
    	    	ProducerRecord<String, Trade> record = new ProducerRecord<String, Trade>(inputTopic, ticker, trade);
    	    	producer.send(record, new Callback() {
    				public void onCompletion(RecordMetadata metadata, Exception exception) {
    					if (exception != null) {
    						System.out.println("There was an error sending trades");
    						exception.printStackTrace();
    					}
    				}
    			});
    		}
    		
    		System.out.println("Ya termine una");
    		Thread.sleep(100); // Every 100 ms is generating new info
    	}
    }
    
	private static Properties getStreamConfiguration(String bootstrapServers) {
		final Properties streamConfiguration = new Properties();
		JsonSerializer<Trade> tradeSerializer = new JsonSerializer<Trade>();
		streamConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		streamConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		streamConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, tradeSerializer.getClass().getName());
		
		return streamConfiguration;
	}
}
