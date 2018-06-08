package com.golaxy.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
/**
 * Mongodb库操作类 
 * @author sunqing
 *
 */

public class ICTMongoDB {
	private MongoClient dbClient 	= null;
	private DB 			dBase 		= null;
	private String		dbName		= "test";				//默认为test库
	private String 		errorMsg 	= "";
	private String 		mongoURI	= "";					//mongo���Ӵ�
	private int 		connectionsPerHost 	= 200;			// ÿ̨�������������?
	private int 		threadsAllowed 		= 10;			// �̶߳����� threadsAllowedToBlockForConnectionMultiplier

	
	public ICTMongoDB() {  

	}
	
	public ICTMongoDB(String mongoURI){
		this.mongoURI = mongoURI;
	}
	
	public ICTMongoDB setMongoURI(String mongoURI){
		this.mongoURI = mongoURI;
		return this;
	}
	
	public ICTMongoDB setConnectionPerHost(int connections){
		this.connectionsPerHost = connections;
		return this;
	}
	
	public ICTMongoDB setThreadsAllowed(int threads){
		this.threadsAllowed = threads;
		return this;
	}
	
	public MongoClient getMongoClient() {
		return dbClient;
	}
	
	public DB getDB() {
		return dBase;
	}
	
	public String getDbName() {
		return dbName;
	}
	
	public DBCollection getCollection(String collection) {
		return dBase.getCollection(collection);
	}
	
	public boolean connect(){
		
		if (mongoURI == null || mongoURI.length() == 0){
			errorMsg = "DBName can not be null";
			return false;
		}
		
		try {
			
			// ���ӳز�������
			MongoClientOptions.Builder optionsBuilder = new MongoClientOptions.Builder();
			optionsBuilder.connectTimeout(20000)
							.socketTimeout(600000)
							.connectionsPerHost(connectionsPerHost)
							.threadsAllowedToBlockForConnectionMultiplier(threadsAllowed);
			
			MongoClientURI uri = new MongoClientURI(mongoURI, optionsBuilder);
			dbClient = new MongoClient(uri);
			if (uri.getDatabase() != null) {
				dbName = uri.getDatabase();
			}
			
			dBase = dbClient.getDB(dbName);
			System.out.println("[ Connect to " + uri.getURI() + " success,  use Databse: " + dbName + " ]");
		} catch (Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public ICTMongoDB useDB(String dbName) {
		this.dbName = dbName;
		dBase = dbClient.getDB(dbName);
		System.out.println("[ Change database to '" + dbName + "' ]");
		return this;
	}
	
	
	public List<BasicDBObject> find(String collName, BasicDBObject filterObj, BasicDBObject selectObj)
		throws MongoException,Exception {
		List<BasicDBObject> results = new ArrayList<BasicDBObject>();
		
		DBCollection dbColl = dBase.getCollection(collName);	
		DBCursor cur = dbColl.find(filterObj, selectObj);

		while (cur.hasNext()) {
			BasicDBObject document = (BasicDBObject) cur.next();
			results.add(document);
		}		
		
		return results;
	}
	
	public List<BasicDBObject> find(String collName, BasicDBObject filterObj, BasicDBObject selectObj, BasicDBObject sortObj)
	 	throws MongoException,Exception {
		List<BasicDBObject> results = new ArrayList<BasicDBObject>();
		
		DBCollection dbColl = dBase.getCollection(collName);	
		DBCursor cur = dbColl.find(filterObj, selectObj).sort(sortObj);

		while (cur.hasNext()) {
			BasicDBObject document = (BasicDBObject) cur.next();
			results.add(document);
		}		
		
		return results;
	}
		
	public List<BasicDBObject> find(String collName, BasicDBObject filterObj, BasicDBObject selectObj, BasicDBObject sortObj, int nlimit)
		throws MongoException,Exception {
		List<BasicDBObject> results = new ArrayList<BasicDBObject>();
		
		DBCollection dbColl = dBase.getCollection(collName);	
		DBCursor cur = dbColl.find(filterObj, selectObj).sort(sortObj).limit(nlimit);
		
		while (cur.hasNext()) {
			BasicDBObject document = (BasicDBObject) cur.next();
			results.add(document);
		}		
		
		return results;
	}
	
	
	public List<BasicDBObject> find(String collName, BasicDBObject filterObj, BasicDBObject selectObj
			, BasicDBObject sortObj, int nskip, int nlimit) throws MongoException,Exception {
		List<BasicDBObject> results = new ArrayList<BasicDBObject>();
		
		DBCollection dbColl = dBase.getCollection(collName);	
		DBCursor cur = dbColl.find(filterObj, selectObj).sort(sortObj).skip(nskip).limit(nlimit);
		
		while (cur.hasNext()) {
			BasicDBObject document = (BasicDBObject) cur.next();
			results.add(document);
		}		
		
		return results;
	}
	
	public List<BasicDBObject> find(String collName, BasicDBObject filterObj, BasicDBObject selectObj, int nlimit)
		throws MongoException,Exception {
		List<BasicDBObject> results = new ArrayList<BasicDBObject>();
		
		DBCollection dbColl = dBase.getCollection(collName);	
		DBCursor cur = dbColl.find(filterObj, selectObj).limit(nlimit);
		
		while (cur.hasNext()) {
			BasicDBObject document = (BasicDBObject) cur.next();
			results.add(document);
		}		
		
		return results;
	}
	
	public List<BasicDBObject> find(String collName, BasicDBObject filterObj, int nlimit)
		throws MongoException,Exception {
		List<BasicDBObject> results = new ArrayList<BasicDBObject>();
		
		DBCollection dbColl = dBase.getCollection(collName);	
		DBCursor cur = dbColl.find(filterObj).limit(nlimit);
		
		while (cur.hasNext()) {
			BasicDBObject document = (BasicDBObject) cur.next();
			results.add(document);
		}		
		
		return results;
	}
	
	public BasicDBObject findOne(String collName, BasicDBObject filterObj) {
		DBCollection dbColl = dBase.getCollection(collName);	
		return (BasicDBObject)dbColl.findOne(filterObj);
	}

	public BasicDBObject findOne(String collName, String _id) {
		DBCollection dbColl = dBase.getCollection(collName);	
		return (BasicDBObject)dbColl.findOne(_id);
	}
	
	public BasicDBObject findOne(String collName, int _id) {
		DBCollection dbColl = dBase.getCollection(collName);	
		return (BasicDBObject)dbColl.findOne(_id);
	}
	
	public BasicDBObject findOne(String collName, long _id) {
		DBCollection dbColl = dBase.getCollection(collName);	
		return (BasicDBObject)dbColl.findOne(_id);
	}
	
	public BasicDBObject findOne(String collName, double _id) {
		DBCollection dbColl = dBase.getCollection(collName);	
		return (BasicDBObject)dbColl.findOne(_id);
	}
	
	
	public BasicDBObject findOne(String collName, ObjectId _id) {
		DBCollection dbColl = dBase.getCollection(collName);	
		return (BasicDBObject)dbColl.findOne(_id);
	}
	
	public BasicDBObject findOne(String collName, Object _id) {
		DBCollection dbColl = dBase.getCollection(collName);	
		return (BasicDBObject)dbColl.findOne(_id);
	}
	
	public int update(String collName, BasicDBObject filterObj, BasicDBObject updateObj){
		try {
			DBCollection dbColl = dBase.getCollection(collName);
			WriteResult result = dbColl.update(filterObj, updateObj, false, true);
			return result.getN();
		} catch (Exception e){
			//e.printStackTrace();
			errorMsg = e.getMessage();
			return -1;
		}
	}
	
	public int update(String collName, Object _id, BasicDBObject updateObj){
		try {
			DBCollection dbColl = dBase.getCollection(collName);
			BasicDBObject filterObj = new BasicDBObject("_id", _id);
			WriteResult result = dbColl.update(filterObj, updateObj, false, true);
			return result.getN();
		} catch (Exception e){
			//e.printStackTrace();
			errorMsg = e.getMessage();
			return -1;
		}
	}
	
	public int update(String collName, BasicDBObject filterObj, BasicDBObject updateObj
			, boolean upsert, boolean multi){
		try {
			DBCollection dbColl = dBase.getCollection(collName);
			WriteResult result = dbColl.update(filterObj, updateObj, upsert, multi);
			return result.getN();
		} catch (Exception e){
			//e.printStackTrace();
			errorMsg = e.getMessage();
			return -1;
		}
	}
	
	public int update(String collName, Object _id, BasicDBObject updateObj
			, boolean upsert, boolean multi){
		try {
			DBCollection dbColl = dBase.getCollection(collName);
			BasicDBObject filterObj = new BasicDBObject("_id", _id);
			WriteResult result = dbColl.update(filterObj, updateObj, upsert, multi);
			return result.getN();
		} catch (Exception e){
			//e.printStackTrace();
			errorMsg = e.getMessage();
			return -1;
		}
	}
	

	
	public int insert(String collName, BasicDBObject obj){
		try {
			DBCollection dbColl = dBase.getCollection(collName);			
			WriteResult result = dbColl.insert(obj);
			return result.getN();
		} catch (Exception e){
			//e.printStackTrace();
			errorMsg = e.getMessage();
			return -1;
		}
	}
	
	public int insert(String collName, List<BasicDBObject> objs){
		try {
			DBCollection dbColl = dBase.getCollection(collName);			
			WriteResult result = dbColl.insert(objs);
			return result.getN();
		} catch (Exception e){
			//e.printStackTrace();
			errorMsg = e.getMessage();
			return -1;
		}
	}
	
	public int delete(String collName, BasicDBObject obj){
		try {
			DBCollection dbColl = dBase.getCollection(collName);
			WriteResult result = dbColl.remove(obj.append("$atomic", true));
			return result.getN();
		} catch (Exception e){
			//e.printStackTrace();
			errorMsg = e.getMessage();
			return -1;
		}
	}
	
	public int delete(String collName, Object _id){
		try {
			DBCollection dbColl = dBase.getCollection(collName);
			BasicDBObject obj = new BasicDBObject("_id", _id);
			WriteResult result = dbColl.remove(obj.append("$atomic", true));
			return result.getN();
		} catch (Exception e){
			//e.printStackTrace();
			errorMsg = e.getMessage();
			return -1;
		}
	}
	
	public void close(){
		dbClient.close();
	}
	
	public String getLastError(){
		return errorMsg;
	}
	
	
	public static void main(String[] args) throws MongoException, Exception{
		ICTMongoDB ictMongo = new ICTMongoDB("mongodb://10.100.1.46:27001/public_weibo");
		ictMongo.connect();
		BasicDBObject filter = new BasicDBObject();
		BasicDBObject select = new BasicDBObject().append("uid", 1).append("sn", 1);
		List<BasicDBObject> results = ictMongo.find("public_weibo", filter, select, 500);
		System.out.println(results);
		for (Map<String, Object> result : results) {
			System.out.print(result.get("_id"));
			System.out.print(",");
		}
		
		ictMongo.close();
		
		
		
		ICTMongoDB ictMongo2 = new ICTMongoDB();
		ictMongo2.setMongoURI("mongodb://10.100.1.46:27001/weibo_tracking").connect();
		//do someing
		
		ictMongo2.close();
		
		
		ICTMongoDB ictMongo3 = new ICTMongoDB();
		ictMongo3.setConnectionPerHost(5).setThreadsAllowed(10).setMongoURI("").connect();
		//do someing
		
		ictMongo2.close();
	}
}
