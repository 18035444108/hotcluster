package com.golaxy.main;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.Logger;
import org.bson.types.BasicBSONList;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.golaxy.util.ConfigData;
import com.golaxy.util.EsUtil;
import com.golaxy.util.ICTMongoDB;
import com.golaxy.util.StreamUtil;
import com.mongodb.BasicDBObject;

public class HotEventClusterNoToHot {
	private static Logger logger = Logger.getLogger(HotEventClusterNoToHot.class);
	private ICTMongoDB ictMongo = new ICTMongoDB();

	public void run() {
		
		ictMongo.setMongoURI(ConfigData.mongoUrl).setConnectionPerHost(2).setThreadsAllowed(5).connect();
		StreamsBuilder builder = new StreamsBuilder();
		
		ArrayList<String> topicList = new ArrayList<>();
		String []topics = ConfigData.kafkaTopic.split(",");
		for (String term : topics) {
			topicList.add(term);
		}
		KStream<String, String> kStream = builder.stream(topicList);

		kStream.foreach(new ForeachAction<String, String>() {
			public void apply(String key, String value) {
				JSONObject kafkaJsonObject = null;
				try {
					kafkaJsonObject = JSON.parseObject(value);
				} catch (Exception e) {
					logger.error("not available jsonstring " + value);
					return;
				}
//
				String _id="";
				if(kafkaJsonObject.containsKey("id")){
					_id = kafkaJsonObject.getString("id").replace("\"", "");
				}else{
					_id = kafkaJsonObject.getString("_id").replace("\"", "");
				}
				int ch = kafkaJsonObject.getInteger("ch");
				//将时间戳设置为整五分钟
				long currentTime = System.currentTimeMillis()/(5*60*1000)*(5*60*1000)+(5*60*1000);
				if((!kafkaJsonObject.containsKey("smid"))|| ConfigData.chNoCreateHot.contains(Integer.toString(ch))){
					// 当消息中不存在smid字段时
					// 先在ES热点索引中组合查询，判断是否是有与已有热点相似的；有，将消息对应ES库总记录新增对应的evid
					JSONObject eventJson = EsUtil.getRecord(kafkaJsonObject, ConfigData.rdEventIndex,
							ConfigData.eventScoreThresh, currentTime - ConfigData.esQueryInterval, "hv", "dc");
					JSONArray evidArray = new JSONArray();
					if (null != eventJson) {
						String index = ConfigData.chToIndexMap.get(ch);
//						JSONObject esJsonObject = EsUtil.selectByIndex(_id, index, "evid", "ch");
						/************************************************************************/
						JSONObject esJsonObject = waitQuery(_id, index, "evid", "ch");
						/***********************************************************************/
						if (null != esJsonObject) {
							String _index = esJsonObject.getString("_index");
							JSONObject sourceJsonObject = esJsonObject.getJSONObject("_source");
							if (sourceJsonObject.containsKey("evid")) {
								if(sourceJsonObject.get("evid") instanceof JSONArray){
									JSONArray evids = sourceJsonObject.getJSONArray("evid");
									if (!evids.contains(eventJson.getString("_id"))) {
										evidArray.add(eventJson.get("_id"));
									}
									JSONObject dataJson = new JSONObject();
									dataJson.put("evid", evidArray);
									if (EsUtil.update(dataJson, _index, _id)) {
										logger.info("----------This record is a hot, update success:" + _id);
									} else {
										logger.error("----------This record is a hot, update failed:" + _id);
									}
									// 根据得到的热点id，更新ES热点库中的dc和hv值 . ---add date
									// 2018-3-21
									ch = sourceJsonObject.getIntValue("ch");
									updateDcAndHv(ch, eventJson);
								}
							}
						}else{
							logger.error("article is not exists, _id is :" + _id);
						}
					}else{
						logger.info("不存在与文章id:"+_id+" 相似的热点!");
					}
				} 
			}

			private JSONObject waitQuery(String _id, String esIndex,String ...fields) {
				for(int i=0;i<3;i++){
					JSONObject esJsonObject = EsUtil.selectByIndex(_id, esIndex, fields);
					if(esJsonObject!=null){
						return esJsonObject;
					}
					logger.error("Not found " + _id);
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				return null;
			}
		});

		Topology to = builder.build();
		KafkaStreams streams = new KafkaStreams(to, StreamUtil.getPropertie());
		streams.start();
	}

	
	/**
	 * 更新热点库中dc和hv值
	 * @param ch
	 * @param eventJson
	 */
	private void updateDcAndHv(int ch, JSONObject eventJson) {
		JSONObject dh = new JSONObject();
		JSONObject sourceJson = eventJson.getJSONObject("_source");
		dh.put("dc", sourceJson.getInteger("dc")+1);
		dh.put("hv", sourceJson.getFloat("hv")+ ConfigData.chWeightMap.get(ch));
		if(EsUtil.update(dh, eventJson.getString("_index"), eventJson.getString("_id"))){
			logger.info("----------update dc and hv success:" + eventJson.getString("_id"));
		}else{
			logger.error("----------update dc and hv failed:" + eventJson.getString("_id"));
		}
	}
	
	/**
	 * 更新evid字段
	 * @param _id
	 * @param evid
	 * @param esJsonObject
	 * @param index
	 */
	private void updateEvid(String _id, String evid, JSONObject esJsonObject, String index) {
		JSONObject sourceJson = esJsonObject.getJSONObject("_source");
		JSONArray evidArray = new JSONArray();
		JSONObject dataJson = new JSONObject();
		if (sourceJson.containsKey("evid")) {
			evidArray = sourceJson.getJSONArray("evid");
			// 判断已有的evid字段是否已经包含该值
			if (!evidArray.contains(evid)) {
				evidArray.add(evid);
			}
			dataJson.put("evid", evidArray);
			if (EsUtil.update(dataJson, index, _id)) {
				logger.info("----------The hot event is exists update recored success:" + _id);
			} else {
				logger.error("----------The hot event is exists update recored failed:" + _id);
			}
		}
	}
	
	private void UpdateBulkEvid(JSONArray jsonArray, String evid) {
		if (EsUtil.updateBulk(jsonArray, evid)) {
			logger.info("批量更新成功！");
		}
	}
	
	public static void main(String[] args) {
		new HotEventClusterNoToHot().run();
	}
}
