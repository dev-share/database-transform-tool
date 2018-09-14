package com.share.swing;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@SuppressWarnings("serial")
public class DataInfo implements Serializable{
	/**
	 * 数据源
	 */
	private Config source;
	/**
	 * 目标源
	 */
	private Config target;
	/**
	 * 库表对应关系映射
	 */
	private Map<String,String> mapping=new TreeMap<String,String>();
	/**
	 * 过滤字段
	 */
	private List<String> filter_columns;
	
	public Config getSource() {
		return source;
	}

	public void setSource(Config source) {
		this.source = source;
	}

	public Config getTarget() {
		return target;
	}

	public void setTarget(Config target) {
		this.target = target;
	}

	public Map<String, String> getMapping() {
		return mapping;
	}

	public void setMapping(Map<String, String> mapping) {
		this.mapping = mapping;
	}

	public List<String> getFilter_columns() {
		return filter_columns;
	}

	public void setFilter_columns(List<String> filter_columns) {
		this.filter_columns = filter_columns;
	}
	/**
	 * @decription 数据配置
	 * @author yi.zhang
	 * @time 2017年7月13日 下午5:19:28
	 * @since 1.0
	 * @jdk	1.8
	 */
	public class Config implements Serializable{
		private String servers;
		private int port=0;
		/**
		 * 资源类型->
		 * 		0:Canal服务,
		 * 		1:Elasticsearch服务,
		 * 		2:NoSQL服务[2.1->Cassandra,2.2->MongoDB,2.3->Redis,2.4->Memecached]
		 * 		3:SQL服务[3.1->MySQL,3.2->SQL　Server,3.3->Oracle]
		 * 		4:数据仓库(Greenplum)
		 * 		5:消息队列(Kafka)
		 */
		private double type = -1;
		private String version = "";
		private String username = "";
		private String password = "";
		private String database = "";
		private String schema = "";
		private String keyspace = "";
		private String other = "";
		private int batch_size = 100;
		
		public String getServers() {
			return servers;
		}
		public void setServers(String servers) {
			this.servers = servers;
		}
		public int getPort() {
			return port;
		}
		public void setPort(int port) {
			this.port = port;
		}
		/**
		 * 资源类型->
		 * 		0:Canal服务,
		 * 		1:Elasticsearch服务,
		 * 		2:NoSQL服务[2.1->Cassandra,2.2->MongoDB,2.3->Redis,2.4->Memecached]
		 * 		3:SQL服务[3.1->MySQL,3.2->SQL　Server,3.3->Oracle]
		 * 		4:数据仓库(Greenplum)
		 * 		5:消息队列(Kafka)
		 */
		public double getType() {
			return type;
		}
		/**
		 * 资源类型->
		 * 		0:Canal服务,
		 * 		1:Elasticsearch服务,
		 * 		2:NoSQL服务[2.1->Cassandra,2.2->MongoDB,2.3->Redis,2.4->Memecached]
		 * 		3:SQL服务[3.1->MySQL,3.2->SQL　Server,3.3->Oracle]
		 * 		4:数据仓库(Greenplum)
		 * 		5:消息队列(Kafka)
		 */
		public void setType(double type) {
			this.type = type;
		}
		public String getVersion() {
			return version;
		}
		public void setVersion(String version) {
			this.version = version;
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
		public String getKeyspace() {
			return keyspace;
		}
		public void setKeyspace(String keyspace) {
			this.keyspace = keyspace;
		}
		public String getOther() {
			return other;
		}
		public void setOther(String other) {
			this.other = other;
		}
		public int getBatch_size() {
			return batch_size;
		}
		public void setBatch_size(int batch_size) {
			this.batch_size = batch_size;
		}
	}
}
