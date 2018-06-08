package com.golaxy.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import javax.sound.sampled.Port;

import org.apache.kafka.streams.state.internals.ChangeLoggingKeyValueBytesStore;
import org.apache.log4j.PropertyConfigurator;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;

import kafka.controller.KafkaController.LogDirEventNotification;

public class ConfigData {
	
	//mongo configurations
	public static final String mongoUrl;
	public static final String mongoTable;

	//kafka configurations
	public static final String kafkaBootStrap;
	/** applicationid */
	public static final String kafkaAppId;
	/**线程数 */
	public static final int kafkaThreads;
	/**偏移量 */
	public static final String kafkaOffset;
	/**topic*/
	public static final String kafkaTopic;
	
	//es configurations
	//es url
	public static final String esUrl;
	/**es index*/
	public static String esIndex;
	/**es event in index*/
	public static final String inEventIndex;
	/**es enent read index */
	public static final String rdEventIndex;
	
	public static final int esQueryMaxNum;
	
	public static final int mongoCThresh;
	
	public static final long mongoEventQueryInterval;
	
	public static final long esQueryInterval;
	
	public static final int kvNumberThresh;
	//各通道对应阈值map
	public static final  HashMap<Integer, Integer> scoreThreshMap = new HashMap<>();
	//es最小score
	public static final double minScore;
	
	public static final double eventScoreThresh;
	
	public static final int boost;
	
	public static final int pageSize;
	
	public static final HashSet<String>  chNoCreateHot = new HashSet<>();
	
	/**权重map*/
	public static final  HashMap<Integer, Float> chWeightMap = new HashMap<>();
	
	public static final HashMap<Integer, String> chToIndexMap = new HashMap<>();
	
	static {
		InputStream in = null;
		try {
			PropertyConfigurator.configure("./conf/log4j.properties");
			in = new FileInputStream("./conf/config.properties");
			Properties prop = new Properties();
			prop.load(new InputStreamReader(in, "utf-8"));
			mongoUrl = prop.getProperty("mongo.url");
			mongoTable = prop.getProperty("mongo.table");
			kafkaBootStrap = prop.getProperty("kafka.bootstrap");
			kafkaAppId = prop.getProperty("kafka.appid");
			kafkaThreads = Integer.parseInt(prop.getProperty("kafka.threads"));
			kafkaOffset = prop.getProperty("kafka.offset");
			
			kafkaTopic = prop.getProperty("kafka.topic");
			esUrl = prop.getProperty("es.url");
			StringBuilder temp = new StringBuilder();
			String []indexs = prop.getProperty("es.index").split(",");
			for(int i=0;i<indexs.length;i++){
				int ch = Integer.parseInt(indexs[i].split(":")[0]);
				String index = indexs[i].split(":")[1];
				temp.append(index);
				if(i!=indexs.length-1){
					temp.append(",");
				}
				chToIndexMap.put(ch,index);
			}
			esIndex=temp.toString();
			inEventIndex = prop.getProperty("es.in.event.index");
			rdEventIndex = prop.getProperty("es.rd.event.index");
			esQueryMaxNum = Integer.parseInt(prop.getProperty("es.query.max.num"));
			
			mongoCThresh = Integer.parseInt(prop.getProperty("mongo.c.thresh"));
			mongoEventQueryInterval  = Long.parseLong(prop.getProperty("mongo.event.query.interval"));
			esQueryInterval = Long.parseLong(prop.getProperty("es.query.interval"));
			kvNumberThresh = Integer.parseInt(prop.getProperty("kv.number.thresh"));
			boost =  Integer.parseInt(prop.getProperty("boost.thresh"));
			
			String []scoreTerm = prop.getProperty("doc.score.thresh").split(",");
			for(int i=0;i<scoreTerm.length;i++){
				int ch = Integer.parseInt(scoreTerm[i].split(":")[0]);
				int score = Integer.parseInt(scoreTerm[i].split(":")[1]);
				scoreThreshMap.put(ch, score);
			}
			 Collection<Integer> c = scoreThreshMap.values();
		     Object[] obj = c.toArray();
		     Arrays.sort(obj);
		     minScore=(int) obj[0];
			eventScoreThresh = Double.parseDouble(prop.getProperty("event.score.thresh"));
			pageSize = Integer.parseInt(prop.getProperty("page.size"));
			
			String []weights = prop.getProperty("doc.channel.weight").split(",");
			for (String string : weights) {
				chWeightMap.put(Integer.parseInt(string.split(":")[0]), Float.parseFloat(string.split(":")[1]));
			}
			
			String []chS = prop.getProperty("ch.no.create.hot").split(",");
			for (String ch : chS) {
				chNoCreateHot.add(ch);
			}
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args) {
		System.out.println(ConfigData.kafkaTopic);
	}

}
