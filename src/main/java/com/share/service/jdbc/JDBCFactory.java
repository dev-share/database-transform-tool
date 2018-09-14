package com.share.service.jdbc;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.druid.pool.xa.DruidXADataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.share.util.StringUtil;
/**
 * @decription 数据库(MySQL|SQL Server|Oracle|Postgresql)服务封装
 * @author yi.zhang
 * @time 2017年6月2日 下午2:14:31
 * @since 1.0
 * @jdk 1.8
 */
public class JDBCFactory {
	private static Logger logger = LogManager.getLogger();
	protected Connection connect = null;
	private String driverName;
	private String url;
	private String username;
	private String password;
	private boolean isDruid;
	private int max_pool_size=10;
	private int init_pool_size=2;
	
	public String getDriverName() {
		return driverName;
	}
	public void setDriverName(String driverName) {
		this.driverName = driverName;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
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
	public boolean isDruid() {
		return isDruid;
	}
	public void setDruid(boolean isDruid) {
		this.isDruid = isDruid;
	}
	public int getMax_pool_size() {
		return max_pool_size;
	}
	public void setMax_pool_size(int max_pool_size) {
		this.max_pool_size = max_pool_size;
	}
	public int getInit_pool_size() {
		return init_pool_size;
	}
	public void setInit_pool_size(int init_pool_size) {
		this.init_pool_size = init_pool_size;
	}
	/**
	 * 描述: 数据库或数据仓库配置
	 * 时间: 2017年11月15日 上午11:35:08
	 * @author yi.zhang
	 * @param driverName	驱动
	 * @param url			URL地址
	 * @param username	用户名
	 * @param password	密码
	 * @param isDruid	是否使用Druid
	 * @param max_pool_size	最大连接池数
	 * @param init_pool_size	最小连接池
	 */
	public void init(String driverName,String url,String username,String password,boolean isDruid,Integer max_pool_size,Integer init_pool_size){
		try {
			if(isDruid){
				@SuppressWarnings("resource")
				DruidXADataSource dataSource = new DruidXADataSource();
				if(!StringUtil.isEmpty(driverName)){
					dataSource.setDriverClassName(driverName);
				}
				dataSource.setUrl(url);
				dataSource.setUsername(username);
				dataSource.setPassword(password);
				if(max_pool_size!=null&&max_pool_size>0){
					dataSource.setMaxActive(max_pool_size);
				}
				if(init_pool_size!=null&&init_pool_size>0){
					dataSource.setInitialSize(init_pool_size);
				}
				dataSource.init();
				connect = dataSource.getConnection();
			}else{
				Class.forName(driverName);
				connect = DriverManager.getConnection(url,username,password);
			}
		} catch (Exception e) {
			logger.error("-----SQL(MySQL|SQL Server|Oracle|Postgresql) Config init Error-----", e);
		}
	}
	/**
	 * 描述: 数据操作(Insert|Update|Delete)
	 * 时间: 2017年11月15日 上午11:27:52
	 * @author yi.zhang
	 * @param sql	sql语句
	 * @param params	参数
	 * @return	返回值
	 */
	public int executeUpdate(String sql,Object...params ){
		try {
			if(connect==null){
				init(driverName, url, username, password, isDruid, max_pool_size, init_pool_size);
			}
			PreparedStatement ps = connect.prepareStatement(sql);
			if(params!=null&&params.length>0){
				for(int i=1;i<=params.length;i++){
					Object value = params[i-1];
					ps.setObject(i, value);
				}
			}
			int result = ps.executeUpdate();
			return result;
		} catch (Exception e) {
			logger.error("-----SQL excute update Error-----", e);
		}
		return -1;
	}
	/**
	 * 描述: 数据库查询(Select)
	 * 时间: 2017年11月15日 上午11:28:42
	 * @author yi.zhang
	 * @param sql	sql语句
	 * @param clazz	映射对象
	 * @param params	占位符参数
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<?> executeQuery(String sql,Class clazz,Object...params){
		try {
			if(connect==null){
				init(driverName, url, username, password, isDruid, max_pool_size, init_pool_size);
			}
			List<Object> list=new ArrayList<Object>();
			PreparedStatement ps = connect.prepareStatement(sql);
			if(params!=null&&params.length>0){
				for(int i=1;i<=params.length;i++){
					Object value = params[i-1];
					ps.setObject(i, value);
				}
			}
			ResultSet rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int count = rsmd.getColumnCount();
			Map<String,String> reflect = new HashMap<String,String>();
			for(int i=1;i<=count;i++){
				String column = rsmd.getColumnName(i);
				String tcolumn = column.replaceAll("_", "");
				if(clazz==null){
					reflect.put(column, column);
				}else{
					Field[] fields = clazz.getDeclaredFields();
					for (Field field : fields) {
						String tfield = field.getName();
						if(tcolumn.equalsIgnoreCase(tfield)){
							reflect.put(column, tfield);
							break;
						}
					}
				}
			}
			while(rs.next()){
				JSONObject obj = new JSONObject();
				for(String column:reflect.keySet()){
					String key = reflect.get(column);
					Object value = rs.getObject(column);
					obj.put(key, value);
				}
				Object object = obj;
				if(clazz!=null){
					object = JSON.parseObject(obj.toJSONString(), clazz);
				}
				list.add(object);
			}
			rs.close();
			ps.close();
			return list;
		} catch (Exception e) {
			logger.error("-----SQL excute query Error-----", e);
		}
		return null;
	}
	/**
	 * 描述: 查询数据表字段名(key:字段名,value:字段类型名)
	 * 时间: 2017年11月15日 上午11:29:32
	 * @author yi.zhang
	 * @param table	表名
	 * @return
	 */
	public Map<String,String> queryColumns(String table){
		try {
			if(connect==null){
				init(driverName, url, username, password, isDruid, max_pool_size, init_pool_size);
			}
			String sql = "select * from "+table;
			PreparedStatement ps = connect.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int count = rsmd.getColumnCount();
			Map<String,String> reflect = new HashMap<String,String>();
			for(int i=1;i<=count;i++){
				String column = rsmd.getColumnName(i);
				String type = rsmd.getColumnTypeName(i);
				reflect.put(column, type);
			}
			rs.close();
			ps.close();
			return reflect;
		} catch (Exception e) {
			logger.error("-----Columns excute query Error-----", e);
		}
		return null;
	}
	/**
	 * 描述: 查询数据库表名
	 * 时间: 2017年11月15日 上午11:29:59
	 * @author yi.zhang
	 * @return 返回表
	 */
	public List<String> queryTables(){
		try {
			String sql = "show tables";
			PreparedStatement ps = connect.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			List<String> tables = new ArrayList<String>();
			while(rs.next()){
				String table = rs.getString(1);
				tables.add(table);
			}
			rs.close();
			ps.close();
			return tables;
		} catch (SQLException e) {
			logger.error("-----Tables excute query Error-----", e);
		}
		return null;
	}
}