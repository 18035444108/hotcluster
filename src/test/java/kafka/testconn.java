package kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import com.golaxy.util.MyTimestampExtractor;

public class testconn {
	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamssimilx");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:29093,localhost:29094");
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyTimestampExtractor.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.setProperty("security.protocol", "SASL_PLAINTEXT");
		props.setProperty("sasl.mechanism", "PLAIN");
		props.setProperty("sasl.kerberos.service.name", "kafka");
		System.setProperty("java.security.auth.login.config", "./conf/kafka_client_jaas.conf");

		StreamsBuilder builder = new StreamsBuilder();
		Topology to = builder.build();

		KStream<String, String> topic = builder.stream("pt_news_doc");
		topic.foreach(new ForeachAction<String, String>() {

			public void apply(String key, String value) {
				System.out.println(value.toString());
			}

		});
		KafkaStreams streams = new KafkaStreams(to, props);
		streams.start();

	}
}
