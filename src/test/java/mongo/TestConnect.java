package mongo;

import org.apache.log4j.Logger;

import com.golaxy.main.HotEventCluster;
import com.golaxy.util.ConfigData;
import com.golaxy.util.ICTMongoDB;
import com.mongodb.BasicDBObject;

public class TestConnect {
	
	private static Logger logger = Logger.getLogger(TestConnect.class);
	public static void main(String[] args) {
		
		
		ICTMongoDB ictMongo = new ICTMongoDB();
		String smid = "011e90a6d00b031e8fed2ba06f1d800b8a";
		ictMongo.setMongoURI("mongodb://localhost:27017/runoob").setConnectionPerHost(2).setThreadsAllowed(5).connect();
		BasicDBObject queryEvid = new BasicDBObject();
		queryEvid.append("_id", "011e90a6d00b031e8fed2ba06f1d800b8a");
		BasicDBObject dataJson = ictMongo.findOne("hot", queryEvid);
		String articles = dataJson.getString("articles");
		System.out.println(articles);
		
		articles=articles+","+"01d00e8c37551d3955dd8f383077029a0c";
		System.out.println(articles);
		BasicDBObject updateObj = new BasicDBObject();
//		updateObj.append("$set", new BasicDBObject().append("articles", articles));
		updateObj.append("$set", new BasicDBObject().append("c", 3));
		
		if (ictMongo.update(ConfigData.mongoTable, smid, updateObj) > 0) {
			logger.info("update C success! " + smid);
		}else{
			logger.info("update C failed! " + smid);
		}
	
	}
//	public static void main(String[] args) {
//		try {
//			//锟斤拷mongo锟斤拷锟捷匡拷锟斤拷锟斤拷
//			MongoClient mongoClient = new MongoClient( "192.168.60.138" , 27017 );
//			//锟斤拷取使锟斤拷db
//			MongoDatabase mongoDatabase = mongoClient.getDatabase("runoob");
//			//锟斤拷取db锟斤拷table
//			MongoIterable<String> iterable = mongoDatabase.listCollectionNames();
//			for (String string : iterable) {
//				System.out.println(string);
//			}
//			//锟斤拷取锟斤拷锟叫碉拷一锟斤拷table
//			MongoCollection<Document> userCol = mongoDatabase.getCollection("hot");
//			userCol.updateOne(Filters.eq("id", "1111"), new Document("$set",new Document("U",true).append("C", 100)));
//			
//			System.out.println("------涓嶅瓨鍦ㄦ椂-------");
//			FindIterable<Document> dataIterable = userCol.find(new Document("id","1111"));
//			System.out.println("====="+dataIterable.first());
//			if(null==dataIterable.first()){
//				System.out.println("涓嶅瓨鍦ㄦ暟鎹�");
//			}
//			for (Document document : dataIterable) {
//				System.out.println(document.toJson());
//			}
//			System.out.println("--------------------");
//			MongoCursor<Document> dataCur = dataIterable.iterator();
//			while(dataCur.hasNext()){
//				System.out.println(dataCur.next().toJson());
//			}
////			
//			System.out.println("---------------and--------------------");
//			BasicDBObject queryObjectAnd = new BasicDBObject().append(QueryOperators.AND, new BasicDBObject[] { new BasicDBObject("id", "1111"),new BasicDBObject("U", true)});
//			FindIterable<Document> dataIterable1 = userCol.find(queryObjectAnd);
//			for (Document document : dataIterable1) {
//				System.out.println(document.toJson());
//			}
//			
//			System.out.println("---------------or-------------");
//			//or锟斤拷锟斤拷锟侥诧拷询
//			BasicDBObject queryObjectOr = new BasicDBObject().append(  
//	                QueryOperators.OR,  
//	                new BasicDBObject[] { new BasicDBObject("id", "1111"),new BasicDBObject("id", "2222")});  
//			dataIterable1 = userCol.find(queryObjectOr);
//			for (Document document : dataIterable1) {
//				System.out.println(document.toJson());
//			}
//			
//		} catch (Exception e) {
//			System.out.println(e.getMessage());
//		}
//	}
}
