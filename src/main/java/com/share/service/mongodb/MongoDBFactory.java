package com.share.service.mongodb;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;

/**
 * @decription MongoDB数据服务封装
 * @author yi.zhang
 * @time 2017年6月2日 下午2:48:49
 * @since 1.0
 * @jdk 1.8
 */
@SuppressWarnings("all")
public class MongoDBFactory {
	private static Logger logger = LogManager.getLogger();
	/**
	 * 主键ID是否处理(true:处理[id],false:不处理[_id])
	 */
	public static boolean ID_HANDLE=false;
	/**
	 * 批量数据大小
	 */
	public static int BATCH_SIZE = 10000;
	/**
	 * 最大时间(单位:毫秒)
	 */
	public static int MAX_WAIT_TIME = 24*60*60*1000;
	
	protected MongoDatabase session = null;
	
	private String servers;
	private String database;
	private String schema;
	private String username;
	private String password;

	public String getServers() {
		return servers;
	}

	public void setServers(String servers) {
		this.servers = servers;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @decription 初始化配置
	 * @author yi.zhang
	 * @time 2017年6月2日 下午2:15:57
	 */
	public void init(String servers,String database,String schema,String username,String password) {
		try {
			List<ServerAddress> saddress = new ArrayList<ServerAddress>();
			if (servers != null && !"".equals(servers)) {
				for (String server : servers.split(",")) {
					String[] address = server.split(":");
					String ip = address[0];
					int port = 27017;
					if (address != null && address.length > 1) {
						port = Integer.valueOf(address[1]);
					}
					saddress.add(new ServerAddress(ip, port));
				}
			}
			MongoCredential credential = MongoCredential.createScramSha1Credential(username, database,password.toCharArray());
			List<MongoCredential> credentials = new ArrayList<MongoCredential>();
			credentials.add(credential);
			Builder builder = new MongoClientOptions.Builder();
			builder.maxWaitTime(MAX_WAIT_TIME);
			// 通过连接认证获取MongoDB连接
			MongoClient client = new MongoClient(saddress, credentials, builder.build());
			// 连接到数据库
			session = client.getDatabase(schema);
		} catch (Exception e) {
			logger.error("-----MongoDB Config init Error-----", e);
		}
	}

	/**
	 * @decription 保存数据
	 * @author yi.zhang
	 * @time 2017年6月2日 下午6:18:49
	 * @param table	文档名称(表名)
	 * @param obj
	 * @return
	 */
	public int save(String table, Object obj) {
		try {
			if(session==null){
				init(servers, database, schema, username, password);
			}
			MongoCollection<Document> collection = session.getCollection(table);
			if (collection == null) {
				session.createCollection(table);
				collection = session.getCollection(table);
			}
			collection.insertOne(Document.parse(JSON.toJSONString(obj)));
			return 1;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}

	/**
	 * @decription 更新数据
	 * @author yi.zhang
	 * @time 2017年6月2日 下午6:19:08
	 * @param table	文档名称(表名)
	 * @param obj
	 * @return
	 */
	public int update(String table, Object obj) {
		try {
			if(session==null){
				init(servers, database, schema, username, password);
			}
			MongoCollection<Document> collection = session.getCollection(table);
			if (collection == null) {
				return 0;
			}
			JSONObject json = JSON.parseObject(JSON.toJSONString(obj));
			Document value = Document.parse(JSON.toJSONString(obj));
			collection.replaceOne(Filters.eq("_id", json.containsKey("_id")?json.get("_id"):json.get("id")), value);
			return 1;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}

	/**
	 * @decription 删除数据
	 * @author yi.zhang
	 * @time 2017年6月2日 下午6:19:25
	 * @param table	文档名称(表名)
	 * @param obj
	 * @return
	 */
	public int delete(String table, Object obj) {
		try {
			if(session==null){
				init(servers, database, schema, username, password);
			}
			MongoCollection<Document> collection = session.getCollection(table);
			if (collection == null) {
				return 0;
			}
			JSONObject json = JSON.parseObject(JSON.toJSONString(obj));
			collection.findOneAndDelete(Filters.eq("_id", json.containsKey("_id")?json.get("_id"):json.get("id")));
			return 1;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}

	/**
	 * @decription 数据库查询
	 * @author yi.zhang
	 * @time 2017年6月26日 下午4:12:59
	 * @param table	文档名称(表名)
	 * @param clazz		映射对象
	 * @param params	参数
	 * @return
	 */
	public List<?> executeQuery(String table, Class clazz, JSONObject params) {
		try {
			if(session==null){
				init(servers, database, schema, username, password);
			}
			MongoCollection<Document> collection = session.getCollection(table);
			if (collection == null) {
				return null;
			}
			List<Object> list = new ArrayList<Object>();
			FindIterable<Document> documents = null;
			if (params != null) {
				List<Bson> filters = new ArrayList<Bson>();
				for (String key : params.keySet()) {
					Object value = params.get(key);
					filters.add(Filters.eq(key, value));
				}
				documents = collection.find(Filters.and(filters));
			} else {
				documents = collection.find();
			}
			MongoCursor<Document> cursor = documents.batchSize(BATCH_SIZE).noCursorTimeout(true).iterator();
			while (cursor.hasNext()) {
				JSONObject obj = new JSONObject();
				Document document = cursor.next();
				for (String column : document.keySet()) {
					Object value = document.get(column);
					if(value instanceof ObjectId){
						value = document.getObjectId(column).toHexString();
					}
					if (clazz == null) {
						obj.put(ID_HANDLE?column.replaceFirst("^(\\_?)", ""):column, value);
					} else {
						String tcolumn = column.replaceAll("_", "");
						Field[] fields = clazz.getDeclaredFields();
						for (Field field : fields) {
							String tfield = field.getName();
							if (column.equalsIgnoreCase(tfield) || tcolumn.equalsIgnoreCase(tfield)) {
								obj.put(tfield, value);
								break;
							}
						}
					}
				}
				Object object = obj;
				if (clazz != null) {
					object = JSON.parseObject(obj.toJSONString(), clazz);
				}
				list.add(object);
			}
			cursor.close();
			return list;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * @decription 查询数据表字段名(key:字段名,value:字段类型名)
	 * @author yi.zhang
	 * @time 2017年6月30日 下午2:16:02
	 * @param table	表名
	 * @return
	 */
	public Map<String,String> queryColumns(String table){
		try {
			if(session==null){
				init(servers, database, schema, username, password);
			}
			MongoCollection<Document> collection = session.getCollection(table);
			if (collection == null) {
				return null;
			}
			Map<String,String> reflect = new HashMap<String,String>();
			FindIterable<Document> documents = collection.find();
			Document document = documents.first();
			if(document==null){
				return reflect;
			}
			for (String column : document.keySet()) {
				Object value = document.get(column);
				String type = "string";
				if(value instanceof Integer){
					type = "int";
				}
				if(value instanceof Long){
					type = "long";
				}
				if(value instanceof Double){
					type = "double";
				}
				if(value instanceof Boolean){
					type = "boolean";
				}
				if(value instanceof Date){
					type = "date";
				}
				reflect.put(column, type);
			}
			return reflect;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * @decription 查询数据库表名
	 * @author yi.zhang
	 * @time 2017年6月30日 下午2:16:02
	 * @param table	表名
	 * @return
	 */
	public List<String> queryTables(){
		try {
			if(session==null){
				init(servers, database, schema, username, password);
			}
			MongoIterable<String> collection = session.listCollectionNames();
			if (collection == null) {
				return null;
			}
			List<String> tables = new ArrayList<String>();
			MongoCursor<String> cursor = collection.iterator();
			while(cursor.hasNext()){
				String table = cursor.next();
				tables.add(table);
			}
			return tables;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}