package com.share.swing;

import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.swing.JFrame;

import com.share.util.DateUtil;
import com.share.util.StringUtil;
/**
 * @decription 公共框架
 * @author yi.zhang
 * @time 2017年7月13日 下午3:27:29
 * @since 1.0
 * @jdk	1.8
 */
public abstract class CommonFrame  extends JFrame implements ActionListener{
	private static final long serialVersionUID = -7858440654032631473L;
	public static String title = ResourceHolder.getProperty("system.name");
	public static String version = ResourceHolder.getProperty("system.version");
	public static String decription = ResourceHolder.getProperty("system.decription");
	public static int width = 800;
	public static int height = 600;
	private static ConcurrentLinkedQueue<String> logger = new ConcurrentLinkedQueue<String>();
	public Container container;
	
	public CommonFrame(){
		_init();
	}
	/**
	 * @decription 初始化信息
	 * @author yi.zhang
	 * @time 2017年7月13日 下午3:27:57
	 */
	public void _init(){
		Toolkit kit = Toolkit.getDefaultToolkit();
		Dimension screen = kit.getScreenSize();
		height = Double.valueOf(screen.getHeight()).intValue();
		width = Double.valueOf(screen.getWidth()).intValue();
//		this.setLocationByPlatform(true);
		this.setSize(width>800?width*3/4:800, height>600?height*3/4:600);
		this.setFont(new Font("Consolas", Font.BOLD, 16));
		this.setLocation((width-this.getSize().width)/2, (height-this.getSize().height)/2);
		this.setBackground(Color.GREEN);
		this.setTitle("®™"+title+" "+ version +" ©ZhangYi");
		this.setVisible(true);
	}
	/**
	 * @decription 处理服务IP与端口
	 * @author yi.zhang
	 * @time 2017年7月21日 下午3:33:01
	 * @param servers	服务拼接地址
	 * @param port		服务端口
	 * @return
	 */
	public String handleServers(String servers,int port){
		String result = "";
		if(port>0&&!StringUtil.isEmpty(servers)){
			if(servers.contains(",")){
				String[] addesses = servers.split(",");
				for (String server : addesses) {
					String[] adress = server.split(":");
					if(adress.length==1){
						result+=(StringUtil.isEmpty(result)?"":",")+adress[0]+":"+port;
					}else{
						result+=(StringUtil.isEmpty(result)?"":",")+server;
					}
				}
			}else{
				result+=servers+":"+port;
			}
		}else{
			result = servers;
		}
		return result;
	}
	/**
	 * @decription 处理资源类型->
	 * 		0:Canal服务,
	 * 		1:Elasticsearch服务,
	 * 		2:NoSQL服务[2.1->Cassandra,2.2->MongoDB,2.3->Redis,2.4->Memecached]
	 * 		3:SQL服务[3.1->MySQL,3.2->SQL　Server,3.3->Oracle]
	 * 		4:数据仓库(Greenplum)
	 * 		5:消息队列(Kafka)
	 * @author yi.zhang
	 * @time 2017年7月21日 下午3:34:29
	 * @param type
	 * @return
	 */
	public String handleType(double type){
		int dtype = Double.valueOf(type).intValue();
		String result= ResourceHolder.getProperty("dstt.ds.type."+type);
		if(StringUtil.isEmpty(result)||type==0.1){
			result = ResourceHolder.getProperty("dstt.ds.type."+dtype);
		}
		return StringUtil.isEmpty(result)?"Unknow":result;
	}
	/**
	 * @decription 处理资源类型->
	 * 		0:Canal服务,
	 * 		1:Elasticsearch服务,
	 * 		2:NoSQL服务[2.1->Cassandra,2.2->MongoDB,2.3->Redis,2.4->Memecached]
	 * 		3:SQL服务[3.1->MySQL,3.2->SQL　Server,3.3->Oracle]
	 * 		4:数据仓库(Greenplum)
	 * 		5:消息队列(Kafka)
	 * @author yi.zhang
	 * @time 2017年7月21日 下午3:34:29
	 * @param type
	 * @return
	 */
	public double handleType(String value){
		double type = -1;
		if(value!=null){
			if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.0"))){
				type = 0;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.0.1"))){
				type = 0.1;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.1"))){
				type = 1;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.2"))){
				type = 2;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.2.1"))){
				type = 2.1;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.2.2"))){
				type = 2.2;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.2.3"))){
				type = 2.3;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.2.4"))){
				type = 2.4;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.3"))){
				type = 3;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.3.1"))){
				type = 3.1;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.3.2"))){
				type = 3.2;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.3.3"))){
				type = 3.3;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.4"))){
				type = 4;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.4.1"))){
				type = 4.1;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.5"))){
				type = 5;
			}else if(value.equalsIgnoreCase(ResourceHolder.getProperty("dstt.ds.type.5.1"))){
				type = 5.1;
			}
		}
		return type;
	}
	/**
	 * @decription 默认端口
	 * @author yi.zhang
	 * @time 2017年7月28日 上午11:55:32
	 * @param type
	 * @return
	 */
	public String handlePort(double type){
		String port = "";
		if(type==0)port=""+11111;
		if(type==0.1)port=""+2181;
		if(type==1)port=""+9300;
		if(type==2.1)port=""+9402;
		if(type==2.2)port=""+27017;
		if(type==2.3)port=""+8888;
		if(type==2.4)port=""+11211;
		if(type==3.1)port=""+3306;
		if(type==3.2)port=""+1433;
		if(type==3.3)port=""+1521;
		if(type==4.1)port=""+5432;
		if(type==5.1)port=""+9092;
		return port;
	}
	@Override
	public void actionPerformed(ActionEvent e) {
		System.out.println(e.getSource());
	}
	
	public static void offer(String msg){
		String data = "["+DateUtil.formatDateTimeStr(new Date())+"]"+msg;
		logger.offer(data);
	}
	
	public static String poll(){
		return logger.poll();
	}
}
