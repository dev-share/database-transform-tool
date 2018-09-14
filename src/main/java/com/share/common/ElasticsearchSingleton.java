package com.share.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.share.service.elasticsearch.http.ElasticsearchExtendHttpFactory;
import com.share.service.elasticsearch.rest.ElasticsearchExtendHighRestFactory;
import com.share.service.elasticsearch.rest.ElasticsearchExtendRestFactory;
import com.share.service.elasticsearch.transport.ElasticsearchExtendTransportFactory;
import com.share.util.StringUtil;
/**
 * 描述: Elasticsearch初始化实例
 * 时间: 2018年1月9日 上午11:18:20
 * @author yi.zhang
 * @since 1.0
 * JDK版本:1.8
 */
public class ElasticsearchSingleton {
	private static Logger logger = LogManager.getLogger();
	private static class InitSingleton{
		private final static ElasticsearchSingleton INSTANCE = new ElasticsearchSingleton();
	}
	private ElasticsearchExtendHttpFactory http;
	private ElasticsearchExtendRestFactory rest;
	private ElasticsearchExtendHighRestFactory high;
	private ElasticsearchExtendTransportFactory transport;
	private ElasticsearchSingleton(){
		try {
			String clusterName = SystemConfig.getProperty(SystemConfig.Config.SERVICE,"elasticsearch.cluster.name");
			String servers = SystemConfig.getProperty("elasticsearch.cluster.servers");
			String username = SystemConfig.getProperty("elasticsearch.cluster.username");
			String password = SystemConfig.getProperty("elasticsearch.cluster.password");
			String http_port = SystemConfig.getProperty("elasticsearch.http.port");
			String transport_port = SystemConfig.getProperty("elasticsearch.transport.port");
			http=http(clusterName, servers, username, password, http_port);
			rest=rest(clusterName, servers, username, password, http_port);
			high=high(clusterName, servers, username, password, http_port);
			transport=transport(clusterName, servers, username, password, transport_port);
		} catch (Exception e) {
			logger.error("--Elasticsearch init Error!",e);
		}
	}
	public final static ElasticsearchSingleton getIntance(){
		return InitSingleton.INSTANCE;
	}
	public ElasticsearchExtendHttpFactory httpES(){
		return http;
	}
	public ElasticsearchExtendRestFactory restES(){
		return rest;
	}
	public ElasticsearchExtendHighRestFactory highES(){
		return high;
	}
	public ElasticsearchExtendTransportFactory transportES(){
		return transport;
	}
	/**
	 * 描述: Elasticsearch配置[Http接口]
	 * 时间: 2018年1月9日 上午11:02:08
	 * @author yi.zhang
	 * @param clusterName	集群名
	 * @param servers		服务地址(多地址以','分割)
	 * @param username		认证用户名
	 * @param password		认证密码
	 * @param port			服务端口
	 * @return
	 */
	private ElasticsearchExtendHttpFactory http(String clusterName,String servers,String username,String password,String port){
		try {
			ElasticsearchExtendHttpFactory factory = new ElasticsearchExtendHttpFactory(clusterName, servers, username, password);
			if(!StringUtil.isEmpty(port))factory = new ElasticsearchExtendHttpFactory(clusterName, servers, username, password,Integer.valueOf(port));
			factory.init();
			return factory;
		} catch (Exception e) {
			logger.error("--Http Elasticsearch init Error!",e);
		}
		return null;
	}
	/**
	 * 描述: Elasticsearch配置[Rest接口]
	 * 时间: 2018年1月9日 上午11:02:08
	 * @author yi.zhang
	 * @param clusterName	集群名
	 * @param servers		服务地址(多地址以','分割)
	 * @param username		认证用户名
	 * @param password		认证密码
	 * @param port			服务端口
	 * @return
	 */
	private ElasticsearchExtendRestFactory rest(String clusterName,String servers,String username,String password,String port){
		try {
			ElasticsearchExtendRestFactory factory = new ElasticsearchExtendRestFactory(clusterName, servers, username, password);
			if(!StringUtil.isEmpty(port))factory = new ElasticsearchExtendRestFactory(clusterName, servers, username, password,Integer.valueOf(port));
			factory.init();
			return factory;
		} catch (Exception e) {
			logger.error("--Rest Elasticsearch init Error!",e);
		}
		return null;
	}
	/**
	 * 描述: Elasticsearch配置[HighRest接口]
	 * 时间: 2018年1月9日 上午11:02:08
	 * @author yi.zhang
	 * @param clusterName	集群名
	 * @param servers		服务地址(多地址以','分割)
	 * @param username		认证用户名
	 * @param password		认证密码
	 * @param port			服务端口
	 * @return
	 */
	private ElasticsearchExtendHighRestFactory high(String clusterName,String servers,String username,String password,String port){
		try {
			ElasticsearchExtendHighRestFactory factory = new ElasticsearchExtendHighRestFactory(clusterName, servers, username, password);
			if(!StringUtil.isEmpty(port))factory = new ElasticsearchExtendHighRestFactory(clusterName, servers, username, password,Integer.valueOf(port));
			factory.init();
			return factory;
		} catch (Exception e) {
			logger.error("--High Rest Elasticsearch init Error!",e);
		}
		return null;
	}
	/**
	 * 描述: Elasticsearch配置[Transport接口]
	 * 时间: 2018年1月9日 上午11:02:08
	 * @author yi.zhang
	 * @param clusterName	集群名
	 * @param servers		服务地址(多地址以','分割)
	 * @param username		认证用户名
	 * @param password		认证密码
	 * @param port			服务端口
	 * @return
	 */
	private ElasticsearchExtendTransportFactory transport(String clusterName,String servers,String username,String password,String port){
		try {
			ElasticsearchExtendTransportFactory factory=new ElasticsearchExtendTransportFactory(clusterName, servers, username, password);
			if(!StringUtil.isEmpty(port))factory = new ElasticsearchExtendTransportFactory(clusterName, servers, username, password,Integer.valueOf(port));
			factory.init();
			return factory;
		} catch (Exception e) {
			logger.error("--Transport Elasticsearch init Error!",e);
		}
		return null;
	}
}
