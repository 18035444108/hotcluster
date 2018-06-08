package com.golaxy.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MyTimestampExtractor implements TimestampExtractor {

	public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
		// TODO Auto-generated method stub
		return 1;
	}
	
}

	
