package com.golaxy.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import ict.http.HttpClient;
import ict.http.Response;

public class EsUtil {
	private static Logger logger = Logger.getLogger(EsUtil.class);
	private static final HttpClient httpClient;
	static {
		httpClient = HttpClient.createSSLInstance(20, 200, 30000, 40000, "zktjrw", "zktjrw");
	}

	/**
	 * 向ES中新增一条记录
	 * 
	 * @param index
	 * @param jsonObject
	 * @param id
	 * @return
	 */
	public static boolean insert(String index, JSONObject jsonObject, String id) {
		jsonObject.remove("evid");
		jsonObject.remove("smid");
		jsonObject.remove("ava");
		jsonObject.remove("aid");
		jsonObject.remove("aca");
		jsonObject.remove("av");
		jsonObject.remove("aloc");
		jsonObject.remove("avt");
		jsonObject.remove("agen");
		jsonObject.put("it", System.currentTimeMillis());
		String url = ConfigData.esUrl + index + "/document/" + id + "/_create";
		String body = jsonObject.toJSONString();
		Response response = httpClient.post(url, body, "application/json", "UTF-8");
		return 201 == response.getStatusCode() ? true : false;
	}

	/**
	 * 根据_id从ES库中查询记录
	 * 
	 * @param id
	 * @param str
	 *            返回结果中包含的字段
	 * @return
	 */
	public static JSONObject select(String id, String... str) {
		String url = "";
		if (str.length > 0) {
			StringBuilder source = new StringBuilder();
			for (int i = 0; i < str.length; i++) {
				source.append(str[i]);
				if (i != str.length - 1) {
					source.append(",");
				}
			}
			url = ConfigData.esUrl + ConfigData.esIndex + "/document/_search?_source=" + source.toString();
		} else {
			url = ConfigData.esUrl + ConfigData.esIndex + "/document/_search";
		}
		String body = "{\"query\":{\"term\":{\"_id\":\"" + id + "\"}}}";

		Response response = httpClient.post(url, body, "application/json", "UTF-8");
		if (200 == response.getStatusCode()) {
			JSONObject jsonObject = JSON.parseObject(response.getResponseAsString());
			JSONObject jsonHits = jsonObject.getJSONObject("hits");
			JSONArray dataArray = jsonHits.getJSONArray("hits");
			if (dataArray.size() != 0) {
				JSONObject jsonScore = dataArray.getJSONObject(0);
				return jsonScore;
			} else {
				return null;
			}
		} else {
			logger.error(response.getErrorMsg() + "\n" + response.getResponseAsString());
		}
		return null;
	}

	/**
	 * 根据_id从ES库中查询记录
	 * 
	 * @param id
	 * @param str
	 *            返回结果中包含的字段
	 * @return
	 */
	public static JSONObject selectByIndex(String id, String index, String... str) {
		String url = "";
		if (str.length > 0) {
			StringBuilder source = new StringBuilder();
			for (int i = 0; i < str.length; i++) {
				source.append(str[i]);
				if (i != str.length - 1) {
					source.append(",");
				}
			}
			url = ConfigData.esUrl + index + "/document/_search?_source=" + source.toString();
		} else {
			url = ConfigData.esUrl + index + "/document/_search";
		}
		String body = "{\"query\":{\"term\":{\"_id\":\"" + id + "\"}}}";
		// System.out.println(url);
		// System.out.println(body);
		Response response = httpClient.post(url, body, "application/json", "UTF-8");
		// System.out.println(response.getResponseAsString());
		if (200 == response.getStatusCode()) {
			JSONObject jsonObject = JSON.parseObject(response.getResponseAsString());
			JSONObject jsonHits = jsonObject.getJSONObject("hits");
			JSONArray dataArray = jsonHits.getJSONArray("hits");
			if (dataArray.size() != 0) {
				JSONObject jsonScore = dataArray.getJSONObject(0);
				return jsonScore;
			} else {
				return null;
			}
		} else {
			logger.error(response.getErrorMsg() + "\n" + response.getResponseAsString());
		}
		return null;
	}

	/**
	 * 根据id更新该条记录的值
	 * 
	 * @param jsonObject
	 * @param id
	 * @return
	 */
	public static boolean update(JSONObject jsonObject, String index, String id) {
		String url = ConfigData.esUrl + index + "/document/" + id + "/_update?retry_on_conflict=3";
		String body = "{\"doc\":" + jsonObject.toString() + "}";
		Response response = httpClient.post(url, body, "application/json", "UTF-8");
		if (!(200 == response.getStatusCode())) {
			logger.error(response.getErrorMsg() + "," + response.getResponseAsString());
		}
		return 200 == response.getStatusCode() ? true : false;
	}

	/**
	 * 批量更新
	 * 
	 * @param jsonArray
	 * @param evid
	 * @return
	 */
	public static boolean updateBulk(JSONArray jsonArray, String evid) {
		StringBuilder body = new StringBuilder();
		for (int i = 0; i < jsonArray.size(); i++) {
			JSONObject term = jsonArray.getJSONObject(i);
			JSONArray evidArray = new JSONArray();
			JSONObject sourceJson = term.getJSONObject("_source");
			JSONObject dataJson = new JSONObject();
			if (sourceJson.containsKey("evid")) {
				if (sourceJson.get("evid") instanceof JSONArray) {
					evidArray = sourceJson.getJSONArray("evid");
					if (!evidArray.contains(evid)) {
						evidArray.add(evid);
					}
				}
			} else {
				evidArray.add(evid);
			}
			logger.info("_id:" + term.get("_id") + "   evid:" + evid);
			dataJson.put("evid", evidArray);
			HashMap<String, Object> map = new HashMap<>();
			map.put("_index", term.get("_index"));
			map.put("_type", term.get("_type"));
			map.put("_id", term.get("_id"));
			String str = buildEsUpdate(map, dataJson);
			body.append(str);
			if (i % 200 == 0 || i == jsonArray.size() - 1) {
				// 批量更新
				String url = ConfigData.esUrl + "/_bulk";
				Response response = httpClient.post(url, body.toString(), "application/json", "UTF-8");
				body = new StringBuilder();
				if (200 != response.getStatusCode()) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * 获取与文章索引相似的列表，只返回指定字段，默认返回所有
	 * 
	 * @param jsonObject
	 * @param index
	 * @param score
	 * @param size
	 * @param maxNum
	 * @param time
	 * @param fields
	 * @return
	 */
	public static JSONArray getRecords(String smid, JSONObject jsonObject, String index, double score, int size,
			long maxNum, long time, String... fields) {

		JSONArray resultArray = new JSONArray();
		// int ch = jsonObject.getInteger("ch");
		StringBuilder kv = GetKv(jsonObject);
		HashMap<String, Object> map = new HashMap<>();
		map.put("title", jsonObject.get("title"));
		map.put("content", kv);
		String url = ConfigData.esUrl + index + "/document/_search";
		int total = 0;

		String body = buildEsQuery(smid, map, score, 1, size, time, fields);
		System.out.println(url + body);
		Response response = null;
		int statusCode = 0;
		
		for (int i = 0; i < 50; i++) {
			response = httpClient.post(url, body, "application/json", "UTF-8");
			statusCode = response.getStatusCode();
			if (200 == statusCode) {
				break;
			}
		}

		if (200 == statusCode) {
			total = GetArray(resultArray, response);
		} else {
			logger.error(statusCode);
			logger.error("get the article list error!" + response.getResponseAsString() + ",body:" + body);
		}

		if (size < total) {
			int page = (total + size - 1) / size;
			for (int i = 2; i <= page; i++) {
				logger.info("---page:" + i);
				body = buildEsQuery(smid, map, ConfigData.minScore, i, size, time, "ch", "evid");
				System.out.println(url + body);
				for(int j=0;j<50;j++){
					response = httpClient.post(url, body, "application/json", "UTF-8");
					statusCode = response.getStatusCode();
					if(200 == statusCode){
						break;
					}
				}
//				response = httpClient.post(url, body, "application/json", "UTF-8");
//				statusCode = response.getStatusCode();
				if (200 == statusCode) {
					total = GetArray(resultArray, response);
				} 
				if (resultArray.size() >= maxNum) {
					break;
				}
			}
		}
		return resultArray;

	}

	/**
	 * 从热点库中返回一条记录，只返回指定字段，默认返回所有
	 * 
	 * @param jsonObject
	 * @param index
	 * @param score
	 * @param time
	 * @param fields
	 * @return
	 */
	public static JSONObject getRecord(JSONObject jsonObject, String index, double score, long time, String... fields) {

		logger.info("在热点库中查找是否存在已有热点！");
		StringBuilder kv = GetKv(jsonObject);
		HashMap<String, Object> map = new HashMap<>();
		map.put("title", jsonObject.get("title"));
		map.put("content", kv);
		String url = ConfigData.esUrl + index + "/document/_search";
		String body = buildEsQuery(null, map, score, 1, 1, time, fields);
		System.out.println(body);
		Response response = httpClient.post(url, body, "application/json", "UTF-8");
		int statusCode = response.getStatusCode();
		if (200 == statusCode) {
			JSONObject reultJson = JSON.parseObject(response.getResponseAsString());
			JSONObject jsonHits = reultJson.getJSONObject("hits");
			if (null != jsonHits) {
				JSONArray resultArray = jsonHits.getJSONArray("hits");
				if (resultArray.size() != 0) {
					return resultArray.getJSONObject(0);
				} else {
					return null;
				}
			}
		}
		return null;
	}

	/**
	 * 获取达到阈值列表
	 * 
	 * @param resultArray
	 * @param ch
	 * @param response
	 * @return
	 */
	private static int GetArray(JSONArray resultArray, Response response) {
		JSONObject reultJson = JSON.parseObject(response.getResponseAsString());
		JSONObject jsonHits = reultJson.getJSONObject("hits");
		if (null != jsonHits) {
			if (jsonHits.getJSONArray("hits").size() != 0) {
				JSONArray scoreArray = jsonHits.getJSONArray("hits");
				for (int i = 0; i < scoreArray.size(); i++) {
					JSONObject jsonScore = scoreArray.getJSONObject(i);
					JSONObject sourceJson = jsonScore.getJSONObject("_source");
					if (jsonScore.getDouble("_score") >= ConfigData.scoreThreshMap.get(sourceJson.getInteger("ch"))) {
						resultArray.add(jsonScore);
					}
				}
			}
		}
		return jsonHits.getIntValue("total");
	}

	/**
	 * 获取指定数量的kv
	 * 
	 * @param jsonObject
	 * @return
	 */
	private static StringBuilder GetKv(JSONObject jsonObject) {
		JSONArray kvArray = jsonObject.getJSONArray("kv");
		StringBuilder kv = new StringBuilder();
		if (null != kvArray) {
			int size = kvArray.size() > ConfigData.kvNumberThresh ? ConfigData.kvNumberThresh : kvArray.size();
			for (int i = 0; i < size; i++) {
				JSONObject term = kvArray.getJSONObject(i);
				kv.append(term.getString("v")).append(" ");
			}
		}
		return kv;
	}

	/**
	 * 构造ES查询语句
	 * 
	 * @param map
	 * @param time
	 * @param minScore
	 * @return
	 */
	private static String buildEsQuery(String smid, Map<String, Object> map, double minScore, int page, int size,
			long time, String... fields) {
		JSONObject queryJson = new JSONObject();
		JSONObject boolJson = new JSONObject();
		JSONObject mustJson = new JSONObject();
		JSONArray mustArray = new JSONArray();
		JSONArray shouldArray = new JSONArray();
		JSONObject rangeJson = new JSONObject();
		JSONObject ptJson = new JSONObject();
		JSONObject timeJson = new JSONObject();
		JSONObject smidJson = new JSONObject();
		JSONObject boostJson = new JSONObject();
		JSONArray sourceArray = new JSONArray();
		JSONObject smJsonObject = new JSONObject();
		timeJson.put("gte", time);
		ptJson.put("pt", timeJson);
		rangeJson.put("range", ptJson);
		for (Entry<String, Object> entry : map.entrySet()) {
			JSONObject matchJson = new JSONObject();
			JSONObject termJson = new JSONObject();

			termJson.put(entry.getKey(), entry.getValue());
			matchJson.put("match", termJson);
			mustArray.add(matchJson);
		}
		if (null != smid) {
			smidJson.put("value", smid);
			smidJson.put("boost", ConfigData.boost);
			smJsonObject.put("smid", smidJson);
			boostJson.put("term", smJsonObject);
			shouldArray.add(boostJson);
		}
		mustJson.put("must", mustArray);
		mustJson.put("filter", rangeJson);
		mustJson.put("should", shouldArray);
		boolJson.put("bool", mustJson);
		queryJson.put("query", boolJson);
		queryJson.put("min_score", minScore);
		queryJson.put("from", (page - 1) * size);
		queryJson.put("size", size);
		for (String term : fields) {
			sourceArray.add(term);
		}
		queryJson.put("_source", sourceArray);
		return queryJson.toString();
	}

	/**
	 * 构造ES更新语句
	 * 
	 * @param map
	 * @param dataJson
	 * @return
	 */
	private static String buildEsUpdate(Map<String, Object> map, JSONObject dataJson) {
		StringBuilder result = new StringBuilder();
		JSONObject updateJson = new JSONObject();
		JSONObject termJson = new JSONObject();
		for (Entry<String, Object> entry : map.entrySet()) {
			termJson.put(entry.getKey(), entry.getValue());
		}
		JSONObject docJson = new JSONObject();
		updateJson.put("update", termJson);
		docJson.put("doc", dataJson);
		result.append(updateJson.toString()).append("\n").append(docJson.toString()).append("\n");

		return result.toString();
	}

	public static void main(String[] args) {

		String url = "https://localhost:19600/" +"rd_news_event" + "/document/_search";
		String id = "01226d9e907252924fc4e847f4e22a6ba2";
		String body = "{\"query\":{\"term\":{\"_id\":\""+id+"\"}}}";
////		//// String body = "{\"aggs\":}";
////		//// String body = "{\"query\":{\"term\":{\"ch\":3}}}";
////		//// String body =
////		//// "{\"query\":{\"match_all\":{}},\"_source\":[\"title\"]}";
////		//// httpClient.get("https://localhost:19600/rd_news/document/_search?pretty");
//		Response response = httpClient.post(url, body, "application/json", "UTF-8");
//		System.out.println(response.getStatusCode());
//		System.out.println(response.getResponseAsString());
		JSONObject updateJson = new JSONObject();
		JSONArray evidArray = new JSONArray();
		evidArray.add("E0160b826619922be30f93f24549fb22b80");
		updateJson.put("evid", evidArray);
//		updateJson.put("dc", 725);
		System.out.println(update(updateJson, "news_doc_20180606", id));
//		JSONObject jsonObject = JSON.parseObject(response.getResponseAsString());
//		JSONObject jsonHits = (JSONObject) jsonObject.get("hits");
//		//// int count = jsonHits.getInteger("total");
//		JSONArray jsonArray = jsonHits.getJSONArray("hits");
//		JSONArray evidArray = new JSONArray();
//		evidArray.add("E043f44adb8c3e1ead47185333db1d35d2e");
//		for (Object object : jsonArray) {
//			JSONObject json = (JSONObject) object;
//			System.out.println(json.getString("_id"));
//			if (!"045b78e0f3d3f938ac30e1fac9d9d3ed55".equals(json.getString("_id"))) {
//				JSONObject updateJson = (JSONObject) json.get("_source");
//				System.out.println(updateJson);
//
//				updateJson.put("evid", evidArray);
//				System.out.println(update(updateJson, json.getString("_index"), json.getString("_id")));
//			}
//		}

		// JSONObject jsonObject =
		// JSON.parseObject("{\"smid\":\"4141\",\"_id\":\"4444\",\"title\":\"\",\"content\":\"\",\"ch\":1,\"age\":26}");
		// System.out.println(insertLocal("event", jsonObject, "6666"));
		// System.out.println(isExists("015168c74b81c7bd45f92c91b5071ab18a"));
		// delete("1111");
		// System.out.println(selectByIndex("09e4a7e8b55b202e46065754cd63052ed4",
		// "rd_elec_news", "evid"));
		// String kv =
		// "{\"title\":\"中华人名共和国\",\"kv\":[{\"v\":\"中国\",\"w\":1}],\"ch\":3}";
		// System.out.println(getRecord(JSON.parseObject(kv),ConfigData.eventIndex,89l));
		// insertLocal();
		//
		//

		// System.out.println(new JSONArray());
//		 JSONObject jsonObject2 = JSON.parseObject("{\"kv\":"
//		 		+ "[{\"v\":\"伙伴关系\",\"w\":5},{\"v\":\"国民党\",\"w\":7},{\"v\":\"会面\",\"w\":4},{\"v\":\"两岸\",\"w\":5},{\"v\":\"两岸关系\",\"w\":3},{\"v\":\"大陆\",\"w\":5},{\"v\":\"座谈\",\"w\":3},{\"v\":\"民众\",\"w\":4},{\"v\":\"交流\",\"w\":4},{\"v\":\"论坛\",\"w\":4},{\"v\":\"台湾\",\"w\":4},{\"v\":\"应该\",\"w\":5},{\"v\":\"厦门\",\"w\":3},{\"v\":\"国台办\",\"w\":2},{\"v\":\"透过\",\"w\":3},{\"v\":\"窗口\",\"w\":3},{\"v\":\"中央社\",\"w\":2},{\"v\":\"解决\",\"w\":4},{\"v\":\"致词\",\"w\":2},{\"v\":\"共产党\",\"w\":3},{\"v\":\"问题\",\"w\":5},{\"v\":\"台商\",\"w\":2},{\"v\":\"主席\",\"w\":3},{\"v\":\"中国国民党\",\"w\":2},{\"v\":\"主任\",\"w\":3},{\"v\":\"合作\",\"w\":3},{\"v\":\"表示\",\"w\":4},{\"v\":\"海峡\",\"w\":2},{\"v\":\"协助\",\"w\":2},{\"v\":\"九二\",\"w\":1},{\"v\":\"建立\",\"w\":3},{\"v\":\"国务院台湾事务办公室\",\"w\":1},{\"v\":\"率团参加\",\"w\":1},{\"v\":\"一家亲\",\"w\":1},{\"v\":\"同文同种\",\"w\":1},{\"v\":\"刘二人\",\"w\":1},{\"v\":\"大会\",\"w\":2},{\"v\":\"两岸人民\",\"w\":1},{\"v\":\"中国\",\"w\":4},{\"v\":\"会面时\",\"w\":1},{\"v\":\"振兴中华\",\"w\":1},{\"v\":\"共同\",\"w\":2},{\"v\":\"子女教育\",\"w\":1},{\"v\":\"台独\",\"w\":1},{\"v\":\"新闻稿\",\"w\":1},{\"v\":\"率团\",\"w\":1},{\"v\":\"层级\",\"w\":1},{\"v\":\"已达成\",\"w\":1},{\"v\":\"互信\",\"w\":1},{\"v\":\"郝龙斌\",\"w\":10},{\"v\":\"厦门市\",\"w\":1},{\"v\":\"厦门大学\",\"w\":1},{\"v\":\"获得\",\"w\":2},{\"v\":\"相关\",\"w\":2},{\"v\":\"关系\",\"w\":2},{\"v\":\"国共两党\",\"w\":1},{\"v\":\"开场\",\"w\":1},{\"v\":\"波折\",\"w\":1},{\"v\":\"师生\",\"w\":1},{\"v\":\"国共\",\"w\":1},{\"v\":\"更进一步\",\"w\":1},{\"v\":\"期盼\",\"w\":1},{\"v\":\"今天上午\",\"w\":1},{\"v\":\"最能\",\"w\":1},{\"v\":\"做起\",\"w\":1},{\"v\":\"记者\",\"w\":2},{\"v\":\"全国政协\",\"w\":1},{\"v\":\"汪洋\",\"w\":1},{\"v\":\"互动\",\"w\":1},{\"v\":\"会见\",\"w\":1},{\"v\":\"重申\",\"w\":1},{\"v\":\"基层\",\"w\":1},{\"v\":\"呼吁\",\"w\":1},{\"v\":\"共识\",\"w\":1},{\"v\":\"一家人\",\"w\":1},{\"v\":\"优惠政策\",\"w\":1},{\"v\":\"政党\",\"w\":1},{\"v\":\"合法\",\"w\":1},{\"v\":\"立场\",\"w\":1},{\"v\":\"今晚\",\"w\":1},{\"v\":\"日电\",\"w\":1},{\"v\":\"官方\",\"w\":1},{\"v\":\"刘结\",\"w\":7},{\"v\":\"三是\",\"w\":1},{\"v\":\"和平\",\"w\":1},{\"v\":\"做法\",\"w\":1},{\"v\":\"之际\",\"w\":1},{\"v\":\"协会\",\"w\":1},{\"v\":\"老师\",\"w\":1},{\"v\":\"配合\",\"w\":1},{\"v\":\"编辑\",\"w\":1},{\"v\":\"遇到\",\"w\":1},{\"v\":\"民间\",\"w\":1},{\"v\":\"走向\",\"w\":1},{\"v\":\"二是\",\"w\":1},{\"v\":\"下午\",\"w\":1},{\"v\":\"晚上\",\"w\":1},{\"v\":\"协议\",\"w\":1},{\"v\":\"合理\",\"w\":1},{\"v\":\"一是\",\"w\":1}]}");
//		 System.out.println(GetKv(jsonObject2));
		//
		//// System.out.println(selectCount("E01e7b50e56a7f085922cee4898a652f2ec"));
//		 HashMap<String, Object> map = new HashMap<>();
//		 map.put("title", "indexvalue");
//		 map.put("content", "typevalue");
//		 System.out.println(buildEsQuery("",map, 0,3,5,0l,"evid","ch","hv"));

		// JSONObject jsonObject2 = JSON.parseObject("{\"evid\":[\"rr\"]}");
		////
		////
		// System.out.println(buildEsUpdate(map, jsonObject2));
		// StringBuilder stringBuilder = new StringBuilder();
		// String str[]={"1","2","3","4"};
		// StringBuilder source = new StringBuilder();
		// for(int i=0;i<str.length;i++){
		// source.append(str[i]);
		// if(i!=str.length-1){
		// source.append(",");
		// }
		// }
		// System.out.println(source.toString());

		// System.out.println(ConfigData.chNoCreateHot.contains(Integer.toString(2)));

		// JSONObject data = new JSONObject();
		// data.put("loc", "北京");
		// update(data, "news_doc_20180226",
		// "01900b120ebb4ca7620b178ebb2c28c838");

	}
}
