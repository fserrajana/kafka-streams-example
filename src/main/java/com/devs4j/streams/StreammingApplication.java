package com.devs4j.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class StreammingApplication {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		KStreamBuilder builder = new KStreamBuilder();

		KStream<String, String> wordCountsInput = builder.stream("input-topic");
		KTable<String, Long> counts = wordCountsInput.mapValues(String::toLowerCase)
				.flatMapValues(value -> Arrays.asList(value.split(" "))).selectKey((ignoredKey, word) -> word)
				.groupByKey().count("Counts");

		counts.to(Serdes.String(), Serdes.Long(), "output-topic");

		KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
