package com.gbm.kafkaTech;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class WordCount {
	
	static final String inputTopic = "wordcount-input";
	static final String outputTopic = "wordcount-output";	
	
	public static void Run(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamConfiguration = getStreamConfiguration(bootstrapServers);
        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));		
	}
	
	private static void createWordCountStream(StreamsBuilder builder) {
		
		final KStream<String, String> textLines = builder.stream(inputTopic);
		final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
		final KTable<String, Long> wordCounts = textLines
				.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
				.groupBy((key, word) -> word)
				.count();
		wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
	}

	private static Properties getStreamConfiguration(String bootstrapServers) {
		final Properties streamConfiguration = new Properties();
		// This name must be unique in entire Kafka Cluster
		streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-example");
		streamConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-example-client");
		streamConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		
		// Serializer & Deserializer
		streamConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		
		streamConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		streamConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		
		return streamConfiguration;
	}
}
