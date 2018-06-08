package kafka;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.golaxy.util.ConfigData;
import com.golaxy.util.EsUtil;
import com.golaxy.util.MyTimestampExtractor;
import com.golaxy.util.StreamUtil;
import com.mongodb.BasicDBObject;

import ict.http.HttpClient;
import ict.http.Response;

public class TestExistsQuery {
	private static final HttpClient httpClient;
	static {
		httpClient = HttpClient.createSSLInstance(20, 200, 30000, 40000, "zktjrw", "zktjrw");
	}
	private static Logger logger = Logger.getLogger(EsUtil.class);
	public void run(){


		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "hotnews");
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.120.17.70:29092,10.120.17.71:29092,10.120.17.73:29092");
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,MyTimestampExtractor.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.setProperty ("security.protocol", "SASL_PLAINTEXT");  
		props.setProperty("sasl.mechanism", "PLAIN");
		props.setProperty ("sasl.kerberos.service.name", "kafka");
		System.setProperty("java.security.auth.login.config", "./conf/kafka_client_jaas.conf");
	
		StreamsBuilder builder = new StreamsBuilder();
		
		
		KStream<String, String> kStream = builder.stream("fina_multi_doc");

		kStream.foreach(new ForeachAction<String, String>() {
			public void apply(String key, String value) {
				JSONObject kafkaJsonObject = null;
				try {
					kafkaJsonObject = JSON.parseObject(value);
				} catch (Exception e) {
					logger.error("not available jsonstring " + value);
					return;
				}
				String _id = kafkaJsonObject.getString("_id");
				int ch = kafkaJsonObject.getInteger("ch");
				//将时间戳设置为整五分钟
				// 判断消息中是否有smid字段并且不存在不需要的通道
				if((!ConfigData.chNoCreateHot.contains(Integer.toString(ch))) && kafkaJsonObject.containsKey("smid")){
							String esIndex = ConfigData.chToIndexMap.get(ch);
							logger.info("------------");
							try {
								Thread.sleep(150);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							JSONObject esJsonObject = TestExistsQuery.selectByIndex(_id,esIndex,"evid");
//							JSONObject esJsonObject = EsUtil.select(_id,"evid");
							if (null != esJsonObject) {
								System.out.println(esJsonObject);
							} 
							else {
								logger.error("index:"+esIndex);
								logger.error("article is not exists, _id is :" + _id);
							}
					}
				
				}
			
		});

		Topology to = builder.build();
		KafkaStreams streams = new KafkaStreams(to, props);
		streams.start();
	
	}
	
	public static JSONObject selectByIndex(String id,String index,String ...str) {
		String url="";
		if(str.length>0){
			StringBuilder source = new StringBuilder();
			for(int i=0;i<str.length;i++){
				source.append(str[i]);
				if(i!=str.length-1){
					source.append(",");
				}
			}
			url = ConfigData.esUrl + index + "/document/_search?_source="+source.toString();
		}else{
			url = ConfigData.esUrl + index + "/document/_search";
		}
		String body = "{\"query\":{\"term\":{\"_id\":\"" + id + "\"}}}";
		System.out.println(url+body);
		Response response = httpClient.post(url, body, "application/json", "UTF-8");
		if(200 == response.getStatusCode()){
			JSONObject jsonObject = JSON.parseObject(response.getResponseAsString());
			JSONObject jsonHits =  jsonObject.getJSONObject("hits");
			JSONArray dataArray = jsonHits.getJSONArray("hits");
			if (dataArray.size() != 0) {
				JSONObject jsonScore = dataArray.getJSONObject(0);
				return jsonScore;
			}else{
				return null;
			}
		}else{
			logger.error(response.getErrorMsg() + "\n" + response.getResponseAsString());
		}
		return null;
	} 
	public static void main(String[] args) {
		new TestExistsQuery().run();
	}
}
