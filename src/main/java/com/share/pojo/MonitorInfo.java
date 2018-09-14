package com.share.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;
/**
 * @decription 监控数据
 * @author yi.zhang
 * @time 2017年6月1日 上午10:13:38
 * @since 1.0
 * @jdk 1.8
 */
@SuppressWarnings("serial")
public class MonitorInfo implements Serializable{
	/**
	 * 数据库名
	 */
	private String schema;
	/**
	 * 表明
	 */
	private String table;
	/**
	 * 操作类型(Insert|Update|Delete|Select|Create等)
	 */
	private String type;
	/**
	 * 数据库SQL(Create|Drop|Grant|Alter等)
	 */
	private String sql;
	/**
	 * 发生改变的行
	 */
	private List<RowInfo> rows = new ArrayList<RowInfo>();
	
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public List<RowInfo> getRows() {
		return rows;
	}
	public void setRows(List<RowInfo> rows) {
		this.rows = rows;
	}
	public class RowInfo implements Serializable{
		/**
		 * 主键字段
		 */
		private String kid;
		/**
		 * 改变之前字段数据(update|delete)
		 */
		private JSONObject before = new JSONObject();
		/**
		 * 改变之后字段数据(update|insert)
		 */
		private JSONObject after = new JSONObject();
		/**
		 * 发生改变字段数据(update)
		 */
		private JSONObject change = new JSONObject();
		
		public String getKid() {
			return kid;
		}
		public void setKid(String kid) {
			this.kid = kid;
		}
		public JSONObject getBefore() {
			return before;
		}
		public void setBefore(JSONObject before) {
			this.before = before;
		}
		public JSONObject getAfter() {
			return after;
		}
		public void setAfter(JSONObject after) {
			this.after = after;
		}
		public JSONObject getChange() {
			return change;
		}
		public void setChange(JSONObject change) {
			this.change = change;
		}
	}
}
