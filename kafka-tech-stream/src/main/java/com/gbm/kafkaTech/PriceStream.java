package com.gbm.kafkaTech;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class PriceStream 
{	
	static final String inputTopic = "price-input";
	static final String outputTopic = "price-output";
	
	@SuppressWarnings("resource")
	public static void main(String[] args)
    {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamConfiguration = getStreamConfiguration(bootstrapServers);
        final StreamsBuilder builder = new StreamsBuilder();
        
        createPriceMinStream(builder);
        
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
    
	private static void createPriceMinStream(StreamsBuilder builder) {
		final KStream<String, Double> prices = builder.stream(inputTopic);
		final KTable<String, Double> minimum = prices 
				.groupByKey()
                .aggregate(
                		() -> 0d,
                		(aggKey, price, initial) -> price < initial ? price : initial);
		
		minimum.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));
	}

	private static Properties getStreamConfiguration(String bootstrapServers) {
		final Properties streamConfiguration = new Properties();
		
		// This name must be unique in entire Kafka Cluster
		streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "price-example");
		streamConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "price-example-client");
		streamConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		
		// Serializer & Deserializer	
		streamConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass().getName());
				
		return streamConfiguration;
	}
}
