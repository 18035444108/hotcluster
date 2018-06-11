package com.golaxy.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.golaxy.util.ConfigData;
import com.golaxy.util.EsUtil;
import com.golaxy.util.ICTMongoDB;
import com.golaxy.util.StreamUtil;
import com.google.gson.JsonArray;
import com.mongodb.BasicDBObject;

public class HotEventCluster {
	public static Logger logger = Logger.getLogger(HotEventCluster.class);
	public ICTMongoDB ictMongo = new ICTMongoDB();

	public void run() {

		ictMongo.setMongoURI(ConfigData.mongoUrl).setConnectionPerHost(2).setThreadsAllowed(5).connect();
		StreamsBuilder builder = new StreamsBuilder();

		ArrayList<String> topicList = new ArrayList<>();
		String[] topics = ConfigData.kafkaTopic.split(",");
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

				String _id = kafkaJsonObject.getString("_id");
				int ch = kafkaJsonObject.getInteger("ch");

				logger.info("-------_id:" + kafkaJsonObject.getString("_id"));
				logger.info("--------smid:" + kafkaJsonObject.getString("smid"));

				// 将时间戳设置为整五分钟
				long currentTime = System.currentTimeMillis() / (5 * 60 * 1000) * (5 * 60 * 1000) + (5 * 60 * 1000);

				// 判断消息中是否有smid字段并且不存在不需要的通道
				if ((!ConfigData.chNoCreateHot.contains(Integer.toString(ch))) && kafkaJsonObject.containsKey("smid")) {

					String smid = kafkaJsonObject.getString("smid");
					BasicDBObject query = new BasicDBObject();
					/* 在mongo库中根据smid查询近一周内的记录 */
					query.append("_id", smid).append("ct",
							new BasicDBObject("$gte", currentTime - ConfigData.mongoEventQueryInterval));
					BasicDBObject mJsonObject = ictMongo.findOne(ConfigData.mongoTable, query);
					logger.info("mongoJsonObject:" + mJsonObject);

					if (null != mJsonObject) {// 当mongo库中存在对应的记录时
						boolean U = mJsonObject.getBoolean("u");
						Integer count = mJsonObject.getInt("c");
						if (U) {// 当U为true时，将对应的ES库中，新增evid字段
							if (count >= 7) {
								String esIndex = ConfigData.chToIndexMap.get(ch);
								/****************************************************************/
								JSONObject esJsonObject = waitQuery(_id, esIndex, "evid");
								logger.info("当mongo库中的u为true时");
								logger.info("找到对应es库总数据：" + esJsonObject);
								/*********************************************************************/
								if (null != esJsonObject) {
									String evid = mJsonObject.getString("e");
									String index = esJsonObject.getString("_index");
									// 更新ES热点库中dc和hv. ---add date 2018-3-21
									JSONObject eventJson = EsUtil.selectByIndex(evid, ConfigData.rdEventIndex, "dc","hv");
									logger.info("更新对应热点库中数据增加hv和dc" + eventJson);
									logger.info("更新对应的文章_id"+_id);
									if (null != eventJson) {
										if(updateEvid(_id, evid, esJsonObject, index)){
											updateDcAndHv(ch, eventJson);
											logger.info("更新热点dc和文章evid成功！");
										}
									}
								} else {
									logger.error("index:" + esIndex);
									logger.error("article is not exists, _id is :" + _id);
								}
							}
						} else if (++count > ConfigData.mongoCThresh) {// 当达到热点阈值时，生成新的热点
							logger.info("-----------------生成新热点------------------------");
							String evid = "E" + smid;
							BasicDBObject updateObj = new BasicDBObject();
							/* 将smid对应mongo库中的'c'字段增加1,'u'字段更新为true */
							updateObj.append("$set", new BasicDBObject().append("c", count).append("u", true)
									.append("e", evid).append("ut", currentTime));
							// 用E加上文章的_id作为热点索引的_id
							int flag = ictMongo.update(ConfigData.mongoTable, smid, updateObj);
							if (flag > 0) {
								logger.info("new hot event create! " + evid);
							}
							// 从ES库中获取对应热点文章信息
							String sIndex = ConfigData.chToIndexMap.get(Integer.parseInt(smid.substring(0, 2)));
							logger.info("热点文章对应的index:" + sIndex);
							// JSONObject hotJson = EsUtil.selectByIndex(smid,
							// sIndex);
							/***************************************************/
							JSONObject hotJson = waitQuery(smid, sIndex);
							logger.info("---smid:" + smid);
							if (null != hotJson) {
								logger.info("当达到mongo中的阈值时，进行热点的生成，对应的热点文章为:" + hotJson.getString("_id"));
							}
							/****************************************************/
							if (null != hotJson) {
								JSONObject articleSourceJson = hotJson.getJSONObject("_source");
								// 获取该热点对应的ES中的文章列表，并依次对文章新增evid字段
								logger.info("Get a list of similar articles begin.");
								JSONArray articleArray = EsUtil.getRecords(smid, articleSourceJson, ConfigData.esIndex,
										ConfigData.minScore, ConfigData.pageSize, ConfigData.esQueryMaxNum,
										currentTime - ConfigData.esQueryInterval, "evid", "ch", "smid");
								logger.info("找到对应es库中的列表：" + articleArray);
								logger.info("Get a list of similar articles end.");
								int articleSize = articleArray.size();
								if (articleSize != 0) {
									float hotValue = 0f;
									HashSet<String> mongoIds = new HashSet<>();
									for (int i = 0; i < articleArray.size(); i++) {
										JSONObject jsonObject = articleArray.getJSONObject(i);
										JSONObject sourceObject = jsonObject.getJSONObject("_source");
										hotValue = ConfigData.chWeightMap.get(sourceObject.getInteger("ch")) + hotValue;

										if (sourceObject.containsKey("smid")) {
											mongoIds.add(sourceObject.getString("smid"));
										}
									}

									// 找到文章列表中smid对应mongo中_id值的记录，将所有记录中的“u”字段更新为“true”，“e”字段更新为“evid”值
									// --- add date 2018-3-21
									for (String mid : mongoIds) {
										BasicDBObject queryEvid = new BasicDBObject();
										queryEvid.append("_id", mid).append("u", false);
										BasicDBObject updateJson = new BasicDBObject();
										updateJson.append("$set",
												new BasicDBObject().append("u", true).append("e", evid));
										if (ictMongo.update(ConfigData.mongoTable, queryEvid, updateObj) > 0) {
											logger.info("update u and e success!" + mid);
										}
									}
									articleSourceJson.put("dc", articleArray.size());
									articleSourceJson.put("hv", hotValue);
									// 将热点文章新增到ES中热点索引中
									if (EsUtil.insert(ConfigData.inEventIndex, articleSourceJson, evid)) {
										logger.error("==================="+evid+"热点文章的个数：" + articleArray.size());
										UpdateBulkEvid(articleArray, evid);
										logger.info("----------event ES insert success:" + evid);
									} else {
										logger.error("----------event ES insert failed:" + evid);
									}
								}
							} else {
								logger.error("the hot article is not exists, _id is :" + smid);
							}
						} else {
							logger.info("-------------更新monggoc字段");
							BasicDBObject updateObj = new BasicDBObject();
							updateObj.append("$set", new BasicDBObject().append("c", count).append("ut", currentTime));
							if (ictMongo.update(ConfigData.mongoTable, smid, updateObj) > 0) {
								logger.info("update C success! " + smid);
							} else {
								logger.info("update C failed! " + smid);
							}
						}
					} else {// 当mongo库中不存在对应的记录时
						logger.info("----当mongo库中不存在对应的记录时，插入mongo库");
						BasicDBObject dataObj = new BasicDBObject();
						dataObj.append("_id", smid).append("c", 2).append("ct", System.currentTimeMillis()).append("ut",
								currentTime);
						JSONObject eventJson = EsUtil.getRecord(kafkaJsonObject, ConfigData.rdEventIndex,
								ConfigData.eventScoreThresh, currentTime - ConfigData.esQueryInterval, "hv", "dc");
						if (null != eventJson) {
							System.out.println("---------------eventJson:" + eventJson);
						}
						String esIndex = ConfigData.chToIndexMap.get(ch);
						JSONObject esJsonObject = waitQuery(_id, esIndex, "evid");
						
						dataObj.append("u", false);
						if(null != esJsonObject){
							if (null != eventJson) {
								// 当热点索引存在达到阈值的热点时
								dataObj.append("u", true);
								// 获取热点的id
								String evid = eventJson.getString("_id");
								dataObj.append("e", evid);
								// 更新ES热点库中dc和hv. ---add date 2018-3-21
								logger.info("============当mongo库不存在记录时,查看热点库中是否存在，存在进行更新！");
								logger.info("===es对应文章："+esJsonObject);
								if(updateEvid(_id, evid, esJsonObject, esJsonObject.getString("_index"))){
									updateDcAndHv(ch, eventJson);
								}
							} 
						}
						
						if (ictMongo.insert(ConfigData.mongoTable, dataObj) < 0) {
							logger.error("mongo insert failed! id:" + smid + "   " + kafkaJsonObject.getString("pt"));
						} else {
							logger.info("mongo insert success");
						}
					}

				}
			}
		});

		Topology to = builder.build();
		KafkaStreams streams = new KafkaStreams(to, StreamUtil.getPropertie());
		streams.start();
	}

	public JSONObject waitQuery(String _id, String esIndex, String... fields) {
		for (int i = 0; i < 3; i++) {
			JSONObject esJsonObject = EsUtil.selectByIndex(_id, esIndex, fields);
			if (esJsonObject != null) {
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

	/**
	 * 更新热点库中dc和hv值
	 * 
	 * @param ch
	 * @param eventJson
	 */
	public void updateDcAndHv(int ch, JSONObject eventJson) {
		// logger.info("------ch"+ch);
		JSONObject dh = new JSONObject();
		JSONObject sourceJson = eventJson.getJSONObject("_source");
		// logger.info("----------before"+"dc:"+sourceJson.getInteger("dc")+"\t"+"hv"+sourceJson.getFloat("hv"));
		dh.put("dc", sourceJson.getInteger("dc") + 1);
		dh.put("hv", sourceJson.getFloat("hv") + ConfigData.chWeightMap.get(ch));
		if (EsUtil.update(dh, eventJson.getString("_index"), eventJson.getString("_id"))) {
			logger.info("----------update dc and hv success:" + eventJson.getString("_id"));
		} else {
			logger.error("----------update dc and hv failed:" + eventJson.getString("_id"));
		}
	}

	/**
	 * 
	 * 更新evid字段
	 * 
	 * @param _id
	 * @param evid
	 * @param esJsonObject
	 * @param index
	 */
	public boolean updateEvid(String _id, String evid, JSONObject esJsonObject, String index) {
		JSONObject sourceJson = esJsonObject.getJSONObject("_source");
		JSONArray evidArray = new JSONArray();
		JSONObject dataJson = new JSONObject();
		if (sourceJson.containsKey("evid")) {
			if (sourceJson.get("evid") instanceof JSONArray) {
				evidArray = sourceJson.getJSONArray("evid");
			}
			// 判断已有的evid字段是否已经包含该值
			if (!evidArray.contains(evid)) {
				evidArray.add(evid);
			}else{
				return false;
			}
			dataJson.put("evid", evidArray);
		}else{
			dataJson.put("evid", evid);
		}
		if (EsUtil.update(dataJson, index, _id)) {
			logger.info("----------The hot event is exists update recored success:" + _id);
			return true;
		} else {
			logger.error("----------The hot event is exists update recored failed:" + _id);
			return false;
		}
	}

	public void UpdateBulkEvid(JSONArray jsonArray, String evid) {
		if (EsUtil.updateBulk(jsonArray, evid)) {
			logger.info("批量更新成功！");
		}
	}

	public static void main(String[] args) {
		new HotEventCluster().run();
	}
}
