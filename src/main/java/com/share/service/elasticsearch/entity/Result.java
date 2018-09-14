package com.share.service.elasticsearch.entity;

import java.util.Date;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
/**
 * 描述: 返回结果
 * 时间: 2018年1月3日 上午11:47:59
 * @author yi.zhang
 * @since 1.0
 * JDK版本:1.8
 */
public class Result{
	public final static int SUCCESS_CODE = 200;
	public final static int ERROR_CODE = 500;
	public static Error ERROR = new Error(ERROR_CODE,null);
	public static Success SUCCESS = new Success(SUCCESS_CODE,null);
	private int status;
	private Date timestamp = new Date();
	public Result() {
		super();
	}
	public Result(int status) {
		super();
		this.status = status;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public Date getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}
	/**
	 * 描述: 失败信息
	 * 时间: 2018年1月3日 上午11:52:30
	 * @author yi.zhang
	 * @since 1.0
	 * JDK版本:1.8
	 */
	public static class Error extends Result{
		private String error;
		private Object message;
		public Error() {
			super(ERROR_CODE);
		}
		public Error(String error) {
			super(ERROR_CODE);
			this.error = error;
		}
		public Error(int status,String error) {
			super(status);
			this.error = error;
		}
		public Error(String error, Object message) {
			super(ERROR_CODE);
			this.error = error;
			this.message = message;
		}
		public Error(int status,String error, Object message) {
			super(status);
			this.error = error;
			this.message = message;
		}
		public String getError() {
			return error;
		}
		public void setError(String error) {
			this.error = error;
		}
		public Object getMessage() {
			return message;
		}
		public void setMessage(Object message) {
			this.message = message;
		}
		public String toString(){
			return JSON.toJSONString(this, new SerializerFeature[]{SerializerFeature.BrowserCompatible});
		}
	}
	/**
	 * 描述: 一般成功信息
	 * 时间: 2018年1月3日 上午11:52:56
	 * @author yi.zhang
	 * @since 1.0
	 * JDK版本:1.8
	 */
	public static class Success extends Result{
		private Object data;
		public Success() {
			super(SUCCESS_CODE);
		}
		public Success(Object data) {
			super(SUCCESS_CODE);
			this.data = data;
		}
		public Success(int status,Object data) {
			super(status);
			this.data = data;
		}
		public Object getData() {
			return data;
		}
		public void setData(Object data) {
			this.data = data;
		}
		public String toString(){
			return JSON.toJSONString(this, new SerializerFeature[]{SerializerFeature.BrowserCompatible});
		}
	}
	/**
	 * 描述: 分页成功信息
	 * 时间: 2018年1月3日 上午11:53:22
	 * @author yi.zhang
	 * @since 1.0
	 * JDK版本:1.8
	 */
	public static class PSuccess extends Success{
		private int pageNo;
		private int pageSize;
		private long total;
		public PSuccess() {
			super();
		}
		public PSuccess(int pageNo, int pageSize, long total,Object data) {
			super(data);
			this.pageNo = pageNo;
			this.pageSize = pageSize;
			this.total = total;
		}

		public int getPageNo() {
			return pageNo;
		}
		public void setPageNo(int pageNo) {
			this.pageNo = pageNo;
		}
		public int getPageSize() {
			return pageSize;
		}
		public void setPageSize(int pageSize) {
			this.pageSize = pageSize;
		}
		public long getTotal() {
			return total;
		}
		public void setTotal(long total) {
			this.total = total;
		}
		public String toString(){
			return JSON.toJSONString(this, new SerializerFeature[]{SerializerFeature.BrowserCompatible});
		}
	}
}
