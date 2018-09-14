package com.share.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SystemConfig {
	private static Logger log = LogManager.getLogger(SystemConfig.class);
	private static class InitConfig{
		private static Properties ALL_INSTANCE = new Properties();
		private static Map<Config,Properties> LAZY_INSTANCE = new HashMap<Config,Properties>();
	}
	public static enum Config{
		CANAL("canal.properties"),
		SERVICE("config.properties");
		private String file;
		private Config(String file) {
			this.file = file;
		}
		public String getFile() {
			return file;
		}
	}
	public static Properties getInstance(){
		if(!InitConfig.ALL_INSTANCE.isEmpty()){
			return InitConfig.ALL_INSTANCE;
		}
		InputStream in = null; 
		try {
			for(Config conf:Config.values()){
				in = ClassLoader.getSystemResourceAsStream(conf.getFile());
				Properties properties = new Properties();
				properties.load(in);
				InitConfig.ALL_INSTANCE.putAll(properties);
			}
		} catch (IOException e) {
			log.error("--All Properties read error!",e);
		}finally{
			if(in!=null){
				try {
					in.close();
				} catch (Exception e) {
					log.error("--All InputStream read error!",e);
				}
			}
		}
		return InitConfig.ALL_INSTANCE;
	}
	public static Properties getInstance(Config conf){
		if(InitConfig.LAZY_INSTANCE.containsKey(conf)){
			return InitConfig.LAZY_INSTANCE.get(conf);
		}
		InputStream in = null; 
		try {
			in = ClassLoader.getSystemResourceAsStream(conf.getFile());
			Properties properties = new Properties();
			properties.load(in);
			InitConfig.LAZY_INSTANCE.put(conf, properties);
		} catch (IOException e) {
			log.error("--Lazy Properties read error!",e);
		}finally{
			if(in!=null){
				try {
					in.close();
				} catch (Exception e) {
					log.error("--Lazy InputStream read error!",e);
				}
			}
		}
		return InitConfig.LAZY_INSTANCE.get(conf);
	}
	
	public static String getProperty(String key) throws Exception{
		return getInstance().getProperty(key);
	}
	public static String getProperty(Config conf,String key) throws Exception{
		return getInstance(conf).getProperty(key);
	}
}