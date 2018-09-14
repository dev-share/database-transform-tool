package com.share.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HttpUtil {
	private static Logger logger = LogManager.getLogger(HttpUtil.class);
	/**
	 * http客户端
	 */
	private static CloseableHttpClient httpClient = HttpClients.createDefault();
	/**
	 * Get请求
	 */
	public final static String METHOD_GET = "GET";
	/**
	 * Post请求
	 */
	public final static String METHOD_POST = "POST";
	/**
	 * Head请求
	 */
	public final static String METHOD_HEAD = "HEAD";
	/**
	 * Options请求
	 */
	public final static String METHOD_OPTIONS = "OPTIONS";
	/**
	 * Put请求
	 */
	public final static String METHOD_PUT = "PUT";
	/**
	 * Delete请求
	 */
	public final static String METHOD_DELETE = "DELETE";
	/**
	 * Trace请求
	 */
	public final static String METHOD_TRACE = "TRACE";
	/**
	 * @param proxyHost 代理地址
	 * @param port		代理端口
	 * @param account	认证账号
	 * @param password	认证密码
	 */
	public static void auth(String proxyHost,int port,final String account,final String password){
		System.setProperty("https.proxyHost", proxyHost);
		System.setProperty("https.proxyPort", port+"");
		Authenticator.setDefault(new Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication(){
				return new PasswordAuthentication(account, new String(password).toCharArray());
			}
		});
	}
	/**
	 * @description 判断服务连通性
	 * @author yi.zhang
	 * @time 2017年4月19日 下午6:00:40
	 * @param url
	 * @param auth	认证信息(username+":"+password)
	 * @return (true:连接成功,false:连接失败)
	 */
	public static boolean checkConnection(String url,String auth){
		boolean flag = false;
		try {
			HttpURLConnection connection = (HttpURLConnection)new URL(url).openConnection();
			connection.setConnectTimeout(5*1000);
			if(auth!=null&&!"".equals(auth)){
				String authorization = "Basic "+new String(Base64.encodeBase64(auth.getBytes()));
				connection.setRequestProperty("Authorization", authorization);
			}
			connection.connect();
			if(connection.getResponseCode()==HttpURLConnection.HTTP_OK){
				flag = true;
			}
			connection.disconnect();
		}catch (Exception e) {
			logger.error("--Server Connect Error !",e);
		}
		return flag;
	}
	/**
	 * @param url 请求URL
	 * @param method 请求URL
	 * @param param	json参数(post|put)
	 * @param auth	认证信息(username+":"+password)
	 * @return
	 */
	public static String httpRequest(String url,String method,String param,String auth){
		String result = null;
		HttpResponse httpResponse = null;
		try {
			HttpRequestBase http = new HttpGet(url);
			if(method.equalsIgnoreCase(METHOD_POST)){
				http = new HttpPost(url);
				StringEntity body = new StringEntity(param,ContentType.APPLICATION_JSON);
				body.setContentType("application/json");
				((HttpPost)http).setEntity(body);
			}else if(method.equalsIgnoreCase(METHOD_PUT)){
				http = new HttpPut(url);
				StringEntity body = new StringEntity(param,ContentType.APPLICATION_JSON);
				body.setContentType("application/json");
				((HttpPut)http).setEntity(body);
			}else if(method.equalsIgnoreCase(METHOD_DELETE)){
				http = new HttpDelete(url);
			}else if(method.equalsIgnoreCase(METHOD_HEAD)){
				http = new HttpHead(url);
			}else if(method.equalsIgnoreCase(METHOD_OPTIONS)){
				http = new HttpOptions(url);
			}else if(method.equalsIgnoreCase(METHOD_TRACE)){
				http = new HttpTrace(url);
			}
			if(auth!=null&&!"".equals(auth)){
				String authorization = "Basic "+new String(Base64.encodeBase64(auth.getBytes()));
				http.setHeader("Authorization", authorization);
			}
			httpResponse = httpClient.execute(http);
			HttpEntity entity = httpResponse.getEntity();
			result = EntityUtils.toString(entity,Consts.UTF_8);
		}catch (Exception e) {
			logger.error("--http request error !",e);
			result = e.getMessage();
		}finally {
			HttpClientUtils.closeQuietly(httpResponse);
		}
		return result;
	}
	/**
	 * @param url 请求URL
	 * @param method 请求URL
	 * @param param	json参数(post|put)
	 * @return
	 */
	public static String urlRequest(String url,String method,String param,String auth){
		String result = null;
		try {
			HttpURLConnection connection = (HttpURLConnection)new URL(url).openConnection();
			connection.setConnectTimeout(60*1000);
			connection.setRequestMethod(method.toUpperCase());
			if(auth!=null&&!"".equals(auth)){
				String authorization = "Basic "+new String(Base64.encodeBase64(auth.getBytes()));
				connection.setRequestProperty("Authorization", authorization);
			}
			if(param!=null&&!"".equals(param)){
				connection.setDoInput(true);
				connection.setDoOutput(true);
				connection.connect();
				DataOutputStream dos = new DataOutputStream(connection.getOutputStream());
				dos.write(param.getBytes(Consts.UTF_8));
				dos.flush();
				dos.close();
			}else{
				connection.connect();
			}
			if(connection.getResponseCode()==HttpURLConnection.HTTP_OK){
				InputStream in = connection.getInputStream();
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				byte[] buff = new byte[1024];
				int len = 0;
				while((len=in.read(buff, 0, buff.length))>0){
					out.write(buff, 0, len);
				}
				byte[] data = out.toByteArray();
				in.close();
				result = data!=null&&data.length>0?new String(data, Consts.UTF_8):null;
			}else{
				result = "{\"status\":"+connection.getResponseCode()+",\"msg\":\""+connection.getResponseMessage()+"\"}";
			}
			connection.disconnect();
		}catch (Exception e) {
			logger.error("--http request error !",e);
		}
		return result;
	}
	/**
	 * @decription URL编码
	 * @author yi.zhang
	 * @time 2017年9月15日 下午3:33:38
	 * @param target
	 * @return
	 */
	public static String encode(String target){
		String result = target;
		try {
			result = URLEncoder.encode(target, Consts.UTF_8.name());
		} catch (UnsupportedEncodingException e) {
			logger.error("--http encode error !",e);
		}
		return result;
	}
	/**
	 * @decription URL解码
	 * @author yi.zhang
	 * @time 2017年9月15日 下午3:33:38
	 * @param target
	 * @return
	 */
	public static String decode(String target){
		String result = target;
		try {
			result = URLDecoder.decode(target, Consts.UTF_8.name());
		} catch (UnsupportedEncodingException e) {
			logger.error("--http decode error !",e);
		}
		return result;
	}
	
	public static void main(String[] args) {
		String index = "testlog";
		String type = "servicelog";
		String id = "";
		String url = "http://127.0.0.1:9200/"+index+"/"+type;
		if(!"".equals(id)){
			url=url+"/"+id;
		}else{
//			url=url+"/_search";
		}
		String method = "post";
//		String body = "{\"query\":{\"match\":{\"operator\":\"test\"}}}";
		String body = "{\"name\":\"mobile music\",\"operator\":\"10000\",\"content\":\"I like music!\",\"createTime\":\"2017-04-20\"}";
		String result = null;
		String auth="elastic:elastic";
		result = checkConnection("http://127.0.0.1:9200",auth)+"";
		result = httpRequest(url, method, body,null);
		System.out.println(result);
		System.out.println("---------------------------------------------------------");
//		result = urlRequest(url, method, param);
		System.out.println(result);
	}
}

