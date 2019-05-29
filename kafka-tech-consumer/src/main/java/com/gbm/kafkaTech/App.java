package com.gbm.kafkaTech;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;

import com.gbm.kafkaTech.model.Trade;
import com.gbm.kafkaTech.serde.JsonDeserializer;

/**
 * Hello world!
 *
 */
public class App 
{	
	static final String inputTopic = "stocks-input";
	
    public static void main(String[] args)
    {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamConfiguration = getStreamConfiguration(bootstrapServers);
        KafkaConsumer<String, Trade> consumer = new KafkaConsumer<String, Trade>(streamConfiguration);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
        consumer.subscribe(Collections.singletonList(inputTopic));
        
        while (true) {
        	 ConsumerRecords<String,Trade> records = consumer.poll(0);
        	 for (ConsumerRecord<String, Trade> record : records) {
        		 System.out.println("Ahi va: " + record.value());
        	 }
        }
        
        
        
        /*
         * 
         * This is fore Stream Reader
        final StreamsBuilder builder = new StreamsBuilder();
        createStockTradeStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        
        */
    }
    
	private static void createStockTradeStream(StreamsBuilder builder) {
	}

	private static Properties getStreamConfiguration(String bootstrapServers) {
		final Properties streamConfiguration = new Properties();
		
		JsonDeserializer<Trade> tradeDeserializer = new JsonDeserializer<Trade>(Trade.class);
		// This name must be unique in entire Kafka Cluster
		//streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-example");
		//streamConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-example-client");
		streamConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		streamConfiguration.put(ConsumerConfig.CLIENT_ID_CONFIG, "bla-bla");
		streamConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-stock");
		streamConfiguration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		streamConfiguration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, tradeDeserializer.getClass().getName());
		
		// Serializer & Deserializer
		//streamConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		//streamConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		
		//streamConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		//streamConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		
		return streamConfiguration;
	}
}
