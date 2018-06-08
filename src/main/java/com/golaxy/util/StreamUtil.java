package com.golaxy.util;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.golaxy.util.ConfigData;
import com.golaxy.util.MyTimestampExtractor;

/**
 * KafkaStream
 * 
 * @author lx
 *
 */
public class StreamUtil {
	public static Properties getPropertie() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, ConfigData.kafkaAppId);
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, ConfigData.kafkaThreads);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigData.kafkaBootStrap);
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,MyTimestampExtractor.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.setProperty ("security.protocol", "SASL_PLAINTEXT");  
		props.setProperty("sasl.mechanism", "PLAIN");
		props.setProperty ("sasl.kerberos.service.name", "kafka");
		System.setProperty("java.security.auth.login.config", "./conf/kafka_client_jaas.conf");
		return props;
	}
	
}
