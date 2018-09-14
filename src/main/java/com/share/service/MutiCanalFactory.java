package com.share.service;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import com.share.pojo.MonitorInfo;
import com.share.util.DateUtil;
/**
 * @decription Canal服务(MySQL数据库监控)
 * @author yi.zhang
 * @time 2017年6月1日 上午10:09:03
 * @since 1.0
 * @jdk 1.8
 */
public class MutiCanalFactory {
	private static Logger logger = LogManager.getLogger(MutiCanalFactory.class);
	/**
	 * 监控过滤规则(默认所有操作:.*\\..*)
	 * EX:
	 * 1.库db1下所有表:db1\\..*
	 * 2.库db1/库db2下所有表:db1\\..*,db2\\..*
	 * 3.库db1下table1表以及库db2下table2表:db1.table1,db2.table2
	 * 4.以name1开头以及包含name2的所有库表:.*\\.name1.*,.*\\.*.name2.*
	 */
	private static String CANAL_FILTER_REGEX = ".*\\..*";
	/**
	 * 多实例列表连接
	 */
	private static ConcurrentHashMap<String,CanalConnector> cache = new  ConcurrentHashMap<String,CanalConnector>();
	private static int BATCH_SIZE = 1000;
	
	private String destinations;
	private String servers;
	private String username;
	private String password;
	private String filter_regex;
	private boolean isZookeeper;
	private int batch_size;
	
	public String getDestinations() {
		return destinations;
	}
	public void setDestinations(String destinations) {
		this.destinations = destinations;
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
	public String getFilter_regex() {
		return filter_regex;
	}
	public void setFilter_regex(String filter_regex) {
		this.filter_regex = filter_regex;
	}
	public boolean isZookeeper() {
		return isZookeeper;
	}
	public void setZookeeper(boolean isZookeeper) {
		this.isZookeeper = isZookeeper;
	}
	public int getBatch_size() {
		return batch_size;
	}
	public void setBatch_size(int batch_size) {
		this.batch_size = batch_size;
	}
	/**
	 * @description Canal服务配置
	 * @author yi.zhang
	 * @time 2017年4月19日 上午10:38:42
	 * @throws Exception
	 */
	public void init(String destinations,String servers,String username,String password,String filter_regex,boolean isZookeeper,Integer batch_size){
		try {
			if(filter_regex!=null){
				CANAL_FILTER_REGEX = filter_regex;
			}
			if(batch_size!=null){
				BATCH_SIZE = batch_size;
			}
			if(servers==null||"".equals(servers)){
				return;
			}
			if(destinations!=null&&!"".equals(destinations)){
				for(String destination:destinations.split(",")){
					if(destination==null||"".equals(destination)){
						continue;
					}
					CanalConnector connector = null;
					if(isZookeeper){
						connector = CanalConnectors.newClusterConnector(servers, destination, username, password);
					}else{
						List<SocketAddress> addresses = new ArrayList<SocketAddress>();
						for(String address : servers.split(",")){
							String[] ips = address.split(":");
							String ip = ips[0];
							int port=11111;
							if(ips.length>1){
								port = Integer.valueOf(ips[1]);
							}
							addresses.add(new InetSocketAddress(ip, port));
						}
						if(addresses!=null&&addresses.size()==1){
							connector = CanalConnectors.newSingleConnector(addresses.get(0), destination, username, password);
						}else{
							connector = CanalConnectors.newClusterConnector(addresses, destination, username, password);
						}
					}
					connector.connect();
			        connector.subscribe(CANAL_FILTER_REGEX);
			        connector.rollback();
			        cache.put(destination, connector);
				}
			}
		} catch (Exception e) {
			logger.error("-----Muti Canal Config init Error-----", e);
		}
	}
	/**
	 * 关闭服务
	 */
	public static void close(){
		if(!cache.isEmpty()){
			for (CanalConnector connector : cache.values()) {
				connector.disconnect();
			}
		}
	}
	/**
	 * 提交数据
	 * @param batchId
	 */
	public static void ack(CanalConnector connector,long batchId){
		connector.ack(batchId);
	}
	/**
	 * 回滚数据
	 * @param batchId
	 */
	public static void rollback(CanalConnector connector,long batchId){
		connector.rollback(batchId);
	}
	
	public List<MonitorInfo> service(){
		List<MonitorInfo> data = new ArrayList<MonitorInfo>();
		try {
			if(cache==null||cache.isEmpty()){
				init(destinations, servers, username, password, filter_regex, isZookeeper, batch_size);
			}
			if(!cache.isEmpty()){
				for (CanalConnector connector : cache.values()) {
					List<MonitorInfo> list = execute(connector);
					if(list!=null&&list.size()>0){
						data.addAll(list);
					}
				}
			}
		} catch (Exception e) {
			logger.error("--Muti Canal监控失败!",e);
		}
		return data;
	}
	/**
	 * @decription 监控数据
	 * @author yi.zhang
	 * @time 2017年6月1日 上午10:10:52
	 * @return
	 */
	protected List<MonitorInfo> execute(CanalConnector connector){
		List<MonitorInfo> monitors = new ArrayList<MonitorInfo>();
		Message message = connector.getWithoutAck(BATCH_SIZE); // 获取指定数量的数据
        long batchId = message.getId();
        List<Entry> list = message.getEntries();
        if(list!=null&&list.size()>0){
        	for (Entry entry : list) {
        		if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                    continue;
                }
                RowChange event = null;
                try {
                	event = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
                }
                String schema = entry.getHeader().getSchemaName();
                String table = entry.getHeader().getTableName();
                String type = event.hasEventType()?event.getEventType().name():null;
                String sql = event.getSql();
                System.out.println("-----{schema:"+schema+",table:"+table+",type:"+type+",sql:"+sql+"}");
                MonitorInfo monitor = new MonitorInfo();
                monitor.setSchema(schema);
                monitor.setTable(table);
                monitor.setType(type);
                List<MonitorInfo.RowInfo> rows = monitor.getRows();
                for (RowData rowData : event.getRowDatasList()) {
                	MonitorInfo.RowInfo row = monitor.new RowInfo();
                	String kid = null;
                	JSONObject before = row.getBefore();
                	JSONObject after = row.getAfter();
                	JSONObject change = row.getChange();
                	List<Column> cbefores = rowData.getBeforeColumnsList();
                	List<Column> cafters = rowData.getAfterColumnsList();
                    for (Column column : cbefores) {
                    	String key = column.getName();
                    	Object value = column.getValue();
                    	String ctype = column.getMysqlType().toLowerCase();
                    	if(ctype.contains("int")){
                    		if(ctype.contains("bigint")){
                    			value = Long.valueOf(column.getValue());
                    		}else{
                    			value = Integer.valueOf(column.getValue());
                    		}
                    	}
                    	if(ctype.contains("decimal")||ctype.contains("numeric")||ctype.contains("double")||ctype.contains("float")){
                    		value = Double.valueOf(column.getValue());
                    	}
                    	if(ctype.contains("timestamp")||ctype.contains("date")){
                    		if(ctype.contains("timestamp")){
                    			value = DateUtil.formatDateTime(column.getValue());
                    		}else{
                    			value = DateUtil.formatDate(column.getValue());
                    		}
                    	}
                    	boolean update = column.getUpdated();
                    	before.put(key, value);
                    	if(update){
                    		change.put(key, value);
                    	}
                    	if(column.getIsKey()&&kid==null){
                    		kid = key;
                    	}
                        System.out.println("--"+type+"--before----{"+key+ ": " + value + ",update: " + update+","+column.getSqlType()+":"+column.getMysqlType()+":"+column.getLength()+"}");
                    }
                    for (Column column : cafters) {
                    	String key = column.getName();
                    	Object value = column.getValue();
                    	String ctype = column.getMysqlType().toLowerCase();
                    	if(ctype.contains("int")){
                    		if(ctype.contains("bigint")){
                    			value = Long.valueOf(column.getValue());
                    		}else{
                    			value = Integer.valueOf(column.getValue());
                    		}
                    	}
                    	if(ctype.contains("decimal")||ctype.contains("numeric")||ctype.contains("double")||ctype.contains("float")){
                    		value = Double.valueOf(column.getValue());
                    	}
                    	if(ctype.contains("timestamp")||ctype.contains("date")){
                    		if(ctype.contains("timestamp")){
                    			value = DateUtil.formatDateTime(column.getValue());
                    		}else{
                    			value = DateUtil.formatDate(column.getValue());
                    		}
                    	}
                    	boolean update = column.getUpdated();
                    	after.put(key, value);
                    	if(update){
                    		change.put(key, value);
                    	}
                    	if(column.getIsKey()&&kid==null){
                    		kid = key;
                    	}
                        System.out.println("--"+type+"--after----{"+key+ ": " + value + ",update: " + update+"}");
                    }
                    row.setKid(kid);
                    if (event.getEventType() == EventType.DELETE) {
                    	Object id = before.get(kid);
                    	sql += "delete from "+table+" where "+kid+"="+(id instanceof String?"'"+id+"'":id)+";";
                    }
                    if (event.getEventType() == EventType.INSERT) {
                    	String keys = "";
                    	String values = "";
                    	for (String key : after.keySet()) {
                    		Object value = after.get(key);
                    		if(value instanceof Date){
                    			value = DateUtil.formatDateTimeStr((Date)value);
                    		}
							if("".equals(keys)){
								keys = key;
								values = (value instanceof String?"'"+value+"'":value+"");
							}else{
								keys +=',' + key;
								values +=',' + (value instanceof String?"'"+value+"'":value+"");
							}
						}
                    	sql += "insert into "+table+"("+keys+")values("+values+");";
                    }
                    if (event.getEventType() == EventType.UPDATE) {
                    	String set = "";
                    	for (String key : change.keySet()) {
                    		Object value = after.get(key);
                    		if(value instanceof Date){
                    			value = DateUtil.formatDateTimeStr((Date)value);
                    		}
                    		if("".equals(set)){
                    			set = key+"="+(value instanceof String?"'"+value+"'":value+"");
                    		}else{
                    			set +=',' + key+"="+(value instanceof String?"'"+value+"'":value+"");
                    		}
                    	}
                    	Object id = before.get(kid);
                    	sql += "update "+table+" set "+set+" where "+kid+"="+(id instanceof String?"'"+id+"'":id)+";";
                    }
                    rows.add(row);
                }
                monitor.setSql(sql);
                monitors.add(monitor);
			}
        }
       ack(connector,batchId);
       return monitors; 
	}
}