package com.share.service.elasticsearch;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public abstract class AbstractElasticsearchFactory implements ElasticsearchFactory{
	protected static String regex = "[-,:,/\"]";
	
	protected String clusterName="elasticsearch";
	protected String servers="localhost";
	protected String username;
	protected String password;
	protected int port;
	
	public abstract int defaultPort();
	/**
	 * 描述: 字段映射
	 * 时间: 2018年1月9日 上午11:36:54
	 * @author yi.zhang
	 * @param index	索引
	 * @param type	类型
	 * @param clazz	实例
	 * @return
	 */
	public abstract String mapping(String index,String type,@SuppressWarnings("rawtypes") Class clazz);
	
	public AbstractElasticsearchFactory() {
		this.port = defaultPort();
	}
	public AbstractElasticsearchFactory(String servers) {
		this.servers = servers;
		this.port = defaultPort();
	}
	public AbstractElasticsearchFactory(String servers,int port) {
		this.servers = servers;
		this.port = port>0?port:defaultPort();
	}
	public AbstractElasticsearchFactory(String clusterName, String servers,int port) {
		this.clusterName = clusterName;
		this.servers = servers;
		this.port = port>0?port:defaultPort();
	}
	public AbstractElasticsearchFactory(String clusterName, String servers, String username, String password) {
		this.clusterName = clusterName;
		this.servers = servers;
		this.username = username;
		this.password = password;
		this.port = defaultPort();
	}
	public AbstractElasticsearchFactory(String clusterName, String servers, String username, String password,int port) {
		this.clusterName = clusterName;
		this.servers = servers;
		this.username = username;
		this.password = password;
		this.port = port>0?port:defaultPort();
	}
	
	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getServers() {
		return servers;
	}

	public void setServers(String servers) {
		this.servers = servers;
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

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	protected Map<String,Object> reflect(@SuppressWarnings("rawtypes") Class clazz,boolean isCustom){
		Map<String,Object> map = new HashMap<String,Object>();
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			String name = field.getName();
			if(name.equalsIgnoreCase("serialVersionUID")){
				continue;
			}
			String type = field.getType().getSimpleName();
			if(type.equalsIgnoreCase("int")||type.equalsIgnoreCase("Integer")){
				String mapping = "{type: 'integer',index:'not_analyzed'}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else if(type.equalsIgnoreCase("byte")){
				String mapping = "{type: 'byte',index:'not_analyzed'}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else if(type.equalsIgnoreCase("short")){
				String mapping = "{type: 'short',index:'not_analyzed'}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else if(type.equalsIgnoreCase("long")){
				String mapping = "{type: 'long',index:'not_analyzed'}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else if(type.equalsIgnoreCase("boolean")){
				String mapping = "{type: 'boolean',index:'not_analyzed'}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else if(type.equalsIgnoreCase("float")){
				String mapping = "{type: 'float',index:'not_analyzed'}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else if(type.equalsIgnoreCase("double")){
				String mapping = "{type: 'double',index:'not_analyzed'}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else if(type.equalsIgnoreCase("date")||type.equalsIgnoreCase("datetime")){
				String format = "strict_date_optional_time||epoch_millis";
				format += "||basic_date||basic_date_time||basic_date_time_no_millis";
				format += "||basic_time||basic_time_no_millis||basic_t_time||basic_t_time_no_millis";
				format += "||strict_basic_week_date||strict_basic_week_date_time_no_millis";
				format += "||strict_date||strict_date_hour_minute||strict_date_hour_minute_second||strict_date_hour_minute_second_fraction||strict_date_hour_minute_second_millis";
				format += "||strict_date_time||strict_date_time_no_millis";
				format += "||strict_hour_minute||strict_hour_minute_second||strict_hour_minute_second_fraction||strict_hour_minute_second_millis";
				format += "||strict_time||strict_time_no_millis||strict_t_time||strict_t_time_no_millis";
				format += "||strict_week_date||strict_week_date_time||strict_week_date_time_no_millis";
				format += "||strict_year_month_day";
				format += "||yyyy-MM-dd||yyyy-MM-dd HH:mm:ss";
				String mapping = "{type: 'date',index:'not_analyzed',format:'"+format+"'}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else if(type.equalsIgnoreCase("char")||type.equalsIgnoreCase("Character")){
				String mapping = "{type: 'string',index:'not_analyzed'}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else if(type.equalsIgnoreCase("object")){
				String mapping = "{type: 'object'}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else if(type.equalsIgnoreCase("String[]")){
				String mapping = "{type: 'text'}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else if(type.equalsIgnoreCase("string")){
				String mapping = "{type: 'string',index:'analyzed',fields:{en:{type:'string',analyzer:'english'},keyword:{type:'keyword',ignore_above:256}"+(isCustom?",custom:{type:'string',analyzer:'es_analyzer'}":"")+"}}";
				map.put(name, JSON.parseObject(mapping));
				continue;
			}else{
				String mapping = "{properties: "+JSON.toJSONString(reflect(field.getDeclaringClass(),isCustom))+"}";
				map.put(name, JSON.parseObject(mapping));
			}
		}
		return map;
	}
	
	protected JSONObject analyzer(String index){
		String char_filter="{symbol_transform:{type:'mapping',mappings:['&=> and ','||=> or ']}}";
		String filter="{default_stopwords:{type:'stop',stopwords:['a','an','and','are','as','at','be','but','by','for','if','in','into','is','it','no','not','of','on','or','such','that','the','their','then','there','these','they','this','to','was','will','with']}}";
		String analyzer="{es_analyzer:{type:'custom',char_filter:['html_strip','symbol_transform'],tokenizer:'standard',filter:['lowercase','default_stopwords']}}";
		String settings="{settings:{analysis:{char_filter:"+JSON.parseObject(char_filter).toJSONString()+",filter:"+JSON.parseObject(filter).toJSONString()+",analyzer:"+JSON.parseObject(analyzer).toJSONString()+"}}}";
		return JSON.parseObject(settings);
	}
}
