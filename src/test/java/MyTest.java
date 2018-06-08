import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;

public class MyTest {
	public static void main(String[] args) {
//		JSONObject jsonObject2  = JSON.parseObject("{\"kv\": [{\"v\":\"挖坑\",\"w\":2},{\"v\":\"进来\",\"w\":3},{\"v\":\"出去\",\"w\":3},{\"v\":\"已经\",\"w\":3},{\"v\":\"这货\",\"w\":3},{\"v\":\"埋人\",\"w\":2},{\"v\":\"我踏马\",\"w\":2},{\"v\":\"踏马\",\"w\":1}]}");
		JSONArray resultArray = new JSONArray();
		
		JSONObject jsonObject = JSON.parseObject("{\"a\":{\"b\":{}}}");
		JSONObject jsonObject2 = jsonObject.getJSONObject("c");
		System.out.println(jsonObject2.get("b"));
	}
}
