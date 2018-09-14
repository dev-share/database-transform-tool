package com.share.service;

import com.share.service.cassandra.CassandraFactory;
import com.share.service.elasticsearch.http.ElasticsearchHttpFactory;
import com.share.service.elasticsearch.transport.ElasticsearchTransportFactory;
import com.share.service.greenplum.GreenplumFactory;
import com.share.service.jdbc.JDBCFactory;
import com.share.service.kafka.KafkaFactory;
import com.share.service.mongodb.MongoDBFactory;
/**
 * @decription 配置文件获取数据源工厂
 * @author yi.zhang
 * @time 2017年7月31日 下午12:03:10
 * @since 1.0
 * @jdk	1.8
 */
public class DataSourceUtil {
	/**
	 * @decription Cassandra配置
	 * @author yi.zhang
	 * @time 2017年7月31日 下午12:03:45
	 * @return
	 */
	public static CassandraFactory cassandra(){
		try {
			String servers = CanalConfig.getProperty("cassandra.servers");
			String keyspace = CanalConfig.getProperty("cassandra.keyspace");
			String username = CanalConfig.getProperty("cassandra.username");
			String password = CanalConfig.getProperty("cassandra.password");
			CassandraFactory factory = new CassandraFactory();
			factory.init(servers, keyspace, username, password);
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * @decription MongoDB配置
	 * @author yi.zhang
	 * @time 2017年7月31日 下午12:03:45
	 * @return
	 */
	public static MongoDBFactory mongodb(){
		try {
			String servers = CanalConfig.getProperty("mongodb.servers");
			String database = CanalConfig.getProperty("mongodb.database");
			String schema = CanalConfig.getProperty("mongodb.schema");
			String username = CanalConfig.getProperty("mongodb.username");
			String password = CanalConfig.getProperty("mongodb.password");
			MongoDBFactory factory = new MongoDBFactory();
			factory.init(servers, database, schema, username, password);
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * @decription Elasticsearch配置[Http接口]
	 * @author yi.zhang
	 * @time 2017年7月31日 下午12:03:45
	 * @return
	 */
	public static ElasticsearchHttpFactory httpElasticsearch(){
		try {
			String clusterName = CanalConfig.getProperty("elasticsearch.cluster.name");
			String servers = CanalConfig.getProperty("elasticsearch.cluster.servers");
			String username = CanalConfig.getProperty("elasticsearch.cluster.username");
			String password = CanalConfig.getProperty("elasticsearch.cluster.password");
			ElasticsearchHttpFactory factory = new ElasticsearchHttpFactory(clusterName,servers, username, password);
			factory.init();
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * @decription Elasticsearch配置[java接口]
	 * @author yi.zhang
	 * @time 2017年7月31日 下午12:03:45
	 * @return
	 */
	public static ElasticsearchTransportFactory elasticsearch(){
		try {
			String clusterName = CanalConfig.getProperty("elasticsearch.cluster.name");
			String servers = CanalConfig.getProperty("elasticsearch.cluster.servers");
			String username = CanalConfig.getProperty("elasticsearch.cluster.username");
			String password = CanalConfig.getProperty("elasticsearch.cluster.password");
			ElasticsearchTransportFactory factory = new ElasticsearchTransportFactory(clusterName, servers, username, password);
			factory.init();
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * @decription Greenplum配置
	 * @author yi.zhang
	 * @time 2017年7月31日 下午12:03:45
	 * @return
	 */
	public static GreenplumFactory greenplum(){
		try {
			String address = CanalConfig.getProperty("greenplum.address");
			String database = CanalConfig.getProperty("greenplum.database");
			String schema = CanalConfig.getProperty("greenplum.schema");
			String username = CanalConfig.getProperty("greenplum.username");
			String password = CanalConfig.getProperty("greenplum.password");
			boolean isDruid = Boolean.valueOf(CanalConfig.getProperty("jdbc.druid.enabled"));
			Integer max_pool_size = Integer.valueOf(CanalConfig.getProperty("jdbc.druid.max_pool_size"));
			Integer init_pool_size = Integer.valueOf(CanalConfig.getProperty("jdbc.druid.init_pool_size"));
			GreenplumFactory factory = new GreenplumFactory();
			factory.init(address, database, schema, username, password, isDruid, max_pool_size, init_pool_size);
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * @decription JDBC(MySQL|SQL Server|Oracle等)配置
	 * @author yi.zhang
	 * @time 2017年7月31日 下午12:03:45
	 * @return
	 */
	public static JDBCFactory jdbc(){
		try {
			String driverName = CanalConfig.getProperty("jdbc.driver");
			String url = CanalConfig.getProperty("jdbc.url");
			String username = CanalConfig.getProperty("jdbc.username");
			String password = CanalConfig.getProperty("jdbc.password");
			boolean isDruid = Boolean.valueOf(CanalConfig.getProperty("jdbc.druid.enabled"));
			Integer max_pool_size = Integer.valueOf(CanalConfig.getProperty("jdbc.druid.max_pool_size"));
			Integer init_pool_size = Integer.valueOf(CanalConfig.getProperty("jdbc.druid.init_pool_size"));
			JDBCFactory factory = new JDBCFactory();
			factory.init(driverName, url, username, password, isDruid, max_pool_size, init_pool_size);
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * @decription Kafka配置
	 * @author yi.zhang
	 * @time 2017年7月31日 下午12:06:39
	 * @return
	 */
	public static KafkaFactory kafka(){
		try {
			String servers = CanalConfig.getProperty("kafka.servers");// 127.0.0.1:9092
			boolean isZookeeper = Boolean.valueOf(CanalConfig.getProperty("kafka.zookeeper.enabled"));
			String zookeeper_servers = CanalConfig.getProperty("kafka.zookeeper.servers");// 127.0.0.1:9092
			String acks = CanalConfig.getProperty("kafka.productor.acks");
			KafkaFactory factory = new KafkaFactory();
			factory.init(servers, isZookeeper, zookeeper_servers, acks);
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
}
