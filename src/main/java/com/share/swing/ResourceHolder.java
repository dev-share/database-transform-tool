package com.share.swing;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.share.util.StringUtil;


/**
 * <pre>
 * 项目:数据转换
 * 描述:资源配置文件取值
 * 作者:ZhangYi
 * 时间:2016年1月26日 上午9:54:00
 * 版本:dtt_v1.0
 * JDK:1.7.80
 * </pre>
 */
public class ResourceHolder {

	private static final Logger	logger	= Logger.getLogger(ResourceHolder.class);
	public static Locale locale = Locale.getDefault();
	/**
	 * <pre>
	 * 描述:获取资源文件值(messages_en.properties/messages_zh.properties)
	 * 作者:ZhangYi
	 * 时间:2016年1月26日 上午9:52:03
	 * 参数：(参数列表)
	 * @param key		国际化key
	 * @param language	(ZH:中文,EN:英文)
	 * @return
	 * </pre>
	 */
	public static String getProperty(String key) {
		String value = "";
		String file = "/messages_zh_CN.properties";
		if (locale.getLanguage().contains("en")) {
			file = "/messages_en.properties";
		}
		try {
			InputStream stream = ResourceHolder.class.getResourceAsStream(file);
			Properties properties = new Properties();
			properties.load(stream);
			value = properties.getProperty(key);
		} catch (IOException e) {
			logger.error("--资源文件取值失败--", e);
		}
		return value;
	}

	/**
	 * <pre>
	 * 描述:获取本地语言
	 * 作者:ZhangYi
	 * 时间:2015年1月30日 下午1:24:06
	 * 参数：(参数列表)
	 * @param language
	 * @return
	 * </pre>
	 */
	public static Locale getLocale(String language) {
		Locale locale = Locale.CHINESE;
		if (!StringUtil.isEmpty(language)) {
			language = language.toLowerCase();
			if (language.indexOf("en") != -1) {
				locale = Locale.ENGLISH;
			}
			if (language.indexOf("zh_hk") != -1) {
				locale = Locale.TRADITIONAL_CHINESE;
			}
			if (language.indexOf("zh_cn") != -1) {
				locale = Locale.SIMPLIFIED_CHINESE;
			}
		}
		return locale;
	}
}
