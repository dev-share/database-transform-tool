package com.share.swing;

import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.SwingConstants;

import org.apache.commons.lang.StringUtils;

import com.share.swing.DataInfo.Config;
import com.share.util.NumberUtil;
import com.share.util.StringUtil;

public class ConfigDialog extends CommonFrame {
	private static final long serialVersionUID = 6682763556358752679L;
	//修改当前行
	public static int CURRENT_ROW = -1;
	//修改当前对象
	public static DataInfo CURRENT_DATA = new DataInfo();
	public ConfigDialog() {
		super._init();
		container = this.getContentPane();
		container.setForeground(Color.BLACK);
		container.setBackground(new Color(230, 230, 230));
		container.setFont(new Font("仿宋", Font.PLAIN, 16));
		container.setLayout(null);
		this.init();
	}
	
	protected void init(){
		DataInfo current = CURRENT_DATA==null?new DataInfo():CURRENT_DATA;
		Config source = current.getSource()==null?current.new Config():current.getSource();
		Config target = current.getTarget()==null?current.new Config():current.getTarget();
		
		int cwidth = container.getSize().width>0?container.getSize().width:width;
        int cheight = container.getSize().height>0?container.getSize().height:height;
        
		JLabel jsource = new JLabel(ResourceHolder.getProperty("dstt.ds.source"));
		jsource.setHorizontalAlignment(SwingConstants.CENTER);
		jsource.setVerticalAlignment(SwingConstants.CENTER);
		jsource.setBounds((cwidth-100)/4-20,10,100,50);
		jsource.setForeground(Color.RED);
		jsource.setFont(new Font("华文新魏", Font.BOLD, 20));
		container.add(jsource);
		JLabel jtarget = new JLabel(ResourceHolder.getProperty("dstt.ds.target"));
		jtarget.setHorizontalAlignment(SwingConstants.CENTER);
		jtarget.setVerticalAlignment(SwingConstants.CENTER);
		jtarget.setBounds((cwidth-100)*3/4-20,10,100,50);
		jtarget.setForeground(Color.RED);
		jtarget.setFont(new Font("华文新魏", Font.BOLD, 20));
		container.add(jtarget);
		
		final JTextPane stip = new JTextPane();
		final JTextPane ttip = new JTextPane();
		
		String keyspace = ResourceHolder.getProperty("dstt.ds.keyspace");
		final JLabel skeyspace = new JLabel(keyspace);
		final JLabel tkeyspace = new JLabel(keyspace);
		final JTextField keyspace1 = new JTextField();
		final JTextField keyspace2 = new JTextField();
		
		String database = ResourceHolder.getProperty("dstt.ds.database");
		final JLabel sdatabase = new JLabel(database);
		final JLabel tdatabase = new JLabel(database);
		final JTextField database1 = new JTextField();
		final JTextField database2 = new JTextField();
		
		String schema = ResourceHolder.getProperty("dstt.ds.schema");
		final JLabel sschema = new JLabel(schema);
		final JLabel tschema = new JLabel(schema);
		final JTextField schema1 = new JTextField();
		final JTextField schema2 = new JTextField();
		
		String type = ResourceHolder.getProperty("dstt.ds.type");
		JLabel stype = new JLabel(type);
		stype.setHorizontalAlignment(SwingConstants.CENTER);
		stype.setVerticalAlignment(SwingConstants.CENTER);
		stype.setBounds((cwidth-100)/4-100,70,100,30);
		stype.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(stype);
		JComboBox<String> type1 = new JComboBox<String>();
		type1.addItem(ResourceHolder.getProperty("dstt.ds.type.2.1"));
		type1.addItem(ResourceHolder.getProperty("dstt.ds.type.2.2"));
		type1.addItem(ResourceHolder.getProperty("dstt.ds.type.3.1"));
		type1.addItem(ResourceHolder.getProperty("dstt.ds.type.3.2"));
		type1.addItem(ResourceHolder.getProperty("dstt.ds.type.3.3"));
		type1.addItem(ResourceHolder.getProperty("dstt.ds.type.4.1"));
		type1.setSelectedItem(handleType(source.getType()));
		type1.setBounds((cwidth-100)/4+60,70,200,30);
		type1.setFont(new Font("华文新魏", Font.BOLD, 16));
		type1.setName("type_source");
		final JTextField port1 = new JTextField();
		type1.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(ItemEvent e) {
				int state = e.getStateChange();
				if(state==1){
					skeyspace.setVisible(false);
					keyspace1.setVisible(false);
					sdatabase.setVisible(false);
					database1.setVisible(false);
					sschema.setVisible(false);
					schema1.setVisible(false);
					
					double type = handleType((String)e.getItem());
//					System.out.println(type+"----[source]----id:"+e.getID()+",item:"+e.getItem()+",state:"+e.getStateChange()+",param:"+e.paramString());
					int itype = Double.valueOf(type).intValue();
					if(type==2.1){
						skeyspace.setVisible(true);
						keyspace1.setVisible(true);
					}else{
						if(itype==2||itype==3||itype==4){
							sdatabase.setVisible(true);
							database1.setVisible(true);
							if(itype!=3){
								sschema.setVisible(true);
								schema1.setVisible(true);
							}
						}
					}
					if(itype==3||itype==4){
						stip.setVisible(false);
					}else{
						stip.setVisible(true);
					}
					port1.setText(handlePort(type));
				}
			}
		});
		container.add(type1);
		JLabel ttype = new JLabel(type);
		ttype.setHorizontalAlignment(SwingConstants.CENTER);
		ttype.setBounds((cwidth-100)*3/4-100,70,100,30);
		ttype.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(ttype);
		JComboBox<String> type2 = new JComboBox<String>();
		type2.addItem(ResourceHolder.getProperty("dstt.ds.type.2.1"));
		type2.addItem(ResourceHolder.getProperty("dstt.ds.type.2.2"));
		type2.addItem(ResourceHolder.getProperty("dstt.ds.type.3.1"));
		type2.addItem(ResourceHolder.getProperty("dstt.ds.type.3.2"));
		type2.addItem(ResourceHolder.getProperty("dstt.ds.type.3.3"));
		type2.addItem(ResourceHolder.getProperty("dstt.ds.type.4.1"));
		type2.addItem(ResourceHolder.getProperty("dstt.ds.type.1"));
		type2.addItem(ResourceHolder.getProperty("dstt.ds.type.5.1"));
		type1.setSelectedItem(handleType(target.getType()));
		type2.setBounds((cwidth-100)*3/4+60,70,200,30);
		type2.setFont(new Font("华文新魏", Font.BOLD, 16));
		type2.setName("type_target");
		final JTextField port2 = new JTextField();
		type2.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(ItemEvent e) {
				int state = e.getStateChange();
				if(state==1){
					tkeyspace.setVisible(false);
					keyspace2.setVisible(false);
					tdatabase.setVisible(false);
					database2.setVisible(false);
					tschema.setVisible(false);
					schema2.setVisible(false);
					
//					System.out.println("----[target]----id:"+e.getID()+",item:"+e.getItem()+",state:"+e.getStateChange()+",param:"+e.paramString());
					double type = handleType((String)e.getItem());
					int itype = Double.valueOf(type).intValue();
					if(type==2.1){
						tkeyspace.setVisible(true);
						keyspace2.setVisible(true);
					}else{
						if(itype==2||itype==3||itype==4){
							tdatabase.setVisible(true);
							database2.setVisible(true);
							if(itype!=3){
								tschema.setVisible(true);
								schema2.setVisible(true);
							}
						}
					}
					if(itype==3||itype==4){
						ttip.setVisible(false);
					}else{
						ttip.setVisible(true);
					}
					port2.setText(handlePort(type));
				}
			}
		});
		container.add(type2);
		
		String servers = ResourceHolder.getProperty("dstt.ds.servers");
		JLabel sservers = new JLabel(servers);
		sservers.setHorizontalAlignment(SwingConstants.CENTER);
		sservers.setVerticalAlignment(SwingConstants.CENTER);
		sservers.setBounds((cwidth-100)/4-100,130,100,30);
		sservers.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(sservers);
		JTextField servers1 = new JTextField();
		servers1.setBounds((cwidth-100)/4+60,130,200,30);
//		servers1.setFont(new Font("华文新魏", Font.BOLD, 16));
		servers1.setText(source.getServers());
		servers1.setName("servers_source");
		container.add(servers1);
		String tip = ResourceHolder.getProperty("dstt.ds.servers.tip");
		stip.setText(tip);
		stip.setEditable(false);
		stip.setBackground(container.getBackground());
		stip.setBounds((cwidth-100)/4+60+200,130,180,30);
		stip.setForeground(new Color(46,139,87));
		stip.setFont(new Font("华文新魏", Font.PLAIN, 12));
		container.add(stip);
		JLabel tservers = new JLabel(servers);
		tservers.setHorizontalAlignment(SwingConstants.CENTER);
		tservers.setBounds((cwidth-100)*3/4-100,130,100,30);
		tservers.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(tservers);
		ttip.setText(tip);
		ttip.setEditable(false);
		ttip.setBackground(container.getBackground());
		ttip.setBounds((cwidth-100)*3/4+260,130,180,30);
		ttip.setForeground(new Color(46,139,87));
		ttip.setFont(new Font("华文新魏", Font.PLAIN, 12));
		container.add(ttip);
		JTextField servers2 = new JTextField();
		servers2.setBounds((cwidth-100)*3/4+60,130,200,30);
//		servers2.setFont(new Font("华文新魏", Font.BOLD, 16));
		servers2.setText(target.getServers());
		servers2.setName("servers_target");
		container.add(servers2);
		
		String port = ResourceHolder.getProperty("dstt.ds.port");
		JLabel sport = new JLabel(port);
		sport.setHorizontalAlignment(SwingConstants.CENTER);
		sport.setVerticalAlignment(SwingConstants.CENTER);
		sport.setBounds((cwidth-100)/4-100,190,100,30);
		sport.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(sport);
		port1.setBounds((cwidth-100)/4+60,190,200,30);
//		port1.setFont(new Font("华文新魏", Font.BOLD, 16));
		port1.setText(source.getPort()+"");
		port1.setName("port_source");
		container.add(port1);
		JLabel tport = new JLabel(port);
		tport.setHorizontalAlignment(SwingConstants.CENTER);
		tport.setBounds((cwidth-100)*3/4-100,190,100,30);
		tport.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(tport);
		port2.setBounds((cwidth-100)*3/4+60,190,200,30);
//		port2.setFont(new Font("华文新魏", Font.BOLD, 16));
		port2.setText(target.getPort()+"");
		port2.setName("port_target");
		container.add(port2);
		
		String username = ResourceHolder.getProperty("dstt.ds.username");
		JLabel susername = new JLabel(username);
		susername.setHorizontalAlignment(SwingConstants.CENTER);
		susername.setVerticalAlignment(SwingConstants.CENTER);
		susername.setBounds((cwidth-100)/4-100,250,100,30);
		susername.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(susername);
		JTextField username1 = new JTextField();
		username1.setBounds((cwidth-100)/4+60,250,200,30);
//		username1.setFont(new Font("华文新魏", Font.BOLD, 16));
		username1.setText(source.getUsername());
		username1.setName("username_source");
		container.add(username1);
		JLabel tusername = new JLabel(username);
		tusername.setHorizontalAlignment(SwingConstants.CENTER);
		tusername.setBounds((cwidth-100)*3/4-100,250,100,30);
		tusername.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(tusername);
		JTextField username2 = new JTextField();
		username2.setBounds((cwidth-100)*3/4+60,250,200,30);
//		username2.setFont(new Font("华文新魏", Font.BOLD, 16));
		username2.setText(target.getUsername());
		username2.setName("username_target");
		container.add(username2);
		
		String password = ResourceHolder.getProperty("dstt.ds.password");
		JLabel spassword = new JLabel(password);
		spassword.setHorizontalAlignment(SwingConstants.CENTER);
		spassword.setVerticalAlignment(SwingConstants.CENTER);
		spassword.setBounds((cwidth-100)/4-100,310,100,30);
		spassword.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(spassword);
		JTextField password1 = new JTextField();
		password1.setBounds((cwidth-100)/4+60,310,200,30);
//		password1.setFont(new Font("华文新魏", Font.BOLD, 16));
		password1.setText(source.getPassword());
		password1.setName("password_source");
		container.add(password1);
		JLabel tpassword = new JLabel(password);
		tpassword.setHorizontalAlignment(SwingConstants.CENTER);
		tpassword.setBounds((cwidth-100)*3/4-100,310,100,30);
		tpassword.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(tpassword);
		JTextField password2 = new JTextField();
		password2.setBounds((cwidth-100)*3/4+60,310,200,30);
//		password2.setFont(new Font("华文新魏", Font.BOLD, 16));
		password2.setText(target.getPassword());
		password2.setName("password_target");
		container.add(password2);
		
		skeyspace.setHorizontalAlignment(SwingConstants.CENTER);
		skeyspace.setVerticalAlignment(SwingConstants.CENTER);
		skeyspace.setBounds((cwidth-100)/4-100,370,100,30);
		skeyspace.setFont(new Font("华文新魏", Font.BOLD, 16));
		keyspace1.setBounds((cwidth-100)/4+60,370,200,30);
//		keyspace1.setFont(new Font("华文新魏", Font.BOLD, 16));
		keyspace1.setText(source.getKeyspace());
		keyspace1.setName("keyspace_source");
		tkeyspace.setHorizontalAlignment(SwingConstants.CENTER);
		tkeyspace.setBounds((cwidth-100)*3/4-100,370,100,30);
		tkeyspace.setFont(new Font("华文新魏", Font.BOLD, 16));
		keyspace2.setBounds((cwidth-100)*3/4+60,370,200,30);
//		keyspace2.setFont(new Font("华文新魏", Font.BOLD, 16));
		keyspace2.setText(target.getKeyspace());
		keyspace2.setName("keyspace_target");
		container.add(skeyspace);
		container.add(keyspace1);
		container.add(tkeyspace);
		container.add(keyspace2);
		
		sdatabase.setHorizontalAlignment(SwingConstants.CENTER);
		sdatabase.setBounds((cwidth-100)/4-100,370,100,30);
		sdatabase.setFont(new Font("华文新魏", Font.BOLD, 16));
		database1.setBounds((cwidth-100)/4+60,370,200,30);
//		database1.setFont(new Font("华文新魏", Font.BOLD, 16));
		database1.setText(source.getDatabase());
		database1.setName("database_source");
		tdatabase.setHorizontalAlignment(SwingConstants.CENTER);
		tdatabase.setBounds((cwidth-100)*3/4-100,370,100,30);
		tdatabase.setFont(new Font("华文新魏", Font.BOLD, 16));
		database2.setBounds((cwidth-100)*3/4+60,370,200,30);
//		database2.setFont(new Font("华文新魏", Font.BOLD, 16));
		database2.setText(target.getDatabase());
		database2.setName("database_target");
		sdatabase.setVisible(false);
		database1.setVisible(false);
		tdatabase.setVisible(false);
		database2.setVisible(false);
		container.add(sdatabase);
		container.add(database1);
		container.add(tdatabase);
		container.add(database2);
		
		sschema.setHorizontalAlignment(SwingConstants.CENTER);
		sschema.setBounds((cwidth-100)/4-100,430,100,30);
		sschema.setFont(new Font("华文新魏", Font.BOLD, 16));
		schema1.setBounds((cwidth-100)/4+60,430,200,30);
//		schema1.setFont(new Font("华文新魏", Font.BOLD, 16));
		schema1.setText(source.getSchema());
		schema1.setName("schema_source");
		tschema.setHorizontalAlignment(SwingConstants.CENTER);
		tschema.setBounds((cwidth-100)*3/4-100,430,100,30);
		tschema.setFont(new Font("华文新魏", Font.BOLD, 16));
		schema2.setBounds((cwidth-100)*3/4+60,430,200,30);
//		schema2.setFont(new Font("华文新魏", Font.BOLD, 16));
		schema2.setText(target.getSchema());
		schema2.setName("schema_target");
		sschema.setVisible(false);
		schema1.setVisible(false);
		tschema.setVisible(false);
		schema2.setVisible(false);
		container.add(sschema);
		container.add(schema1);
		container.add(tschema);
		container.add(schema2);
		
		String table_source = "";
		String table_target = "";
		if(current.getMapping()!=null&&current.getMapping().size()>0){
			for(String key:current.getMapping().keySet()){
				String value = current.getMapping().get(key);
				table_source = StringUtil.isEmpty(table_source)?key:table_source+","+key;
				table_target = StringUtil.isEmpty(table_target)?value:table_target+","+value;
			}
		}
		JLabel mapping = new JLabel(ResourceHolder.getProperty("dstt.ds.mapping"));
		mapping.setHorizontalAlignment(SwingConstants.CENTER);
		mapping.setVerticalAlignment(SwingConstants.CENTER);
		mapping.setBounds((cwidth-100)/4-100,490,100,30);
		mapping.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(mapping);
		JTextField table1 = new JTextField();
		table1.setBounds((cwidth-100)/4+60,490,200,30);
//		table1.setFont(new Font("华文新魏", Font.BOLD, 16));
		table1.setText(table_source);
		table1.setName("table_source");
		container.add(table1);
		JLabel ttable = new JLabel("----------------------------------------------------------------->");
		ttable.setHorizontalAlignment(SwingConstants.CENTER);
		ttable.setBounds((cwidth-100)*2/4-100,490,(cwidth-100)*3/8,30);
		ttable.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(ttable);
		JTextField table2 = new JTextField();
		table2.setBounds((cwidth-100)*3/4+60,490,200,30);
//		table2.setFont(new Font("华文新魏", Font.BOLD, 16));
		table2.setText(table_target);
		table2.setName("table_target");
		container.add(table2);
		
		
		JLabel filter = new JLabel(ResourceHolder.getProperty("dstt.ds.filter"));
		filter.setHorizontalAlignment(SwingConstants.CENTER);
		filter.setVerticalAlignment(SwingConstants.CENTER);
		filter.setBounds((cwidth-100)/4-100,550,100,30);
		filter.setFont(new Font("华文新魏", Font.BOLD, 16));
		container.add(filter);
		JTextField filter1 = new JTextField();
		filter1.setBounds((cwidth-100)/4+60,550,(cwidth-100)*3/8,30);
//		filter1.setFont(new Font("华文新魏", Font.BOLD, 16));
		filter1.setText(current.getFilter_columns()!=null&&current.getFilter_columns().size()>0?StringUtils.join(current.getFilter_columns(), ","):"");
		filter1.setName("filter_source");
		container.add(filter1);
		JTextPane ftip = new JTextPane();
		ftip.setText(ResourceHolder.getProperty("dstt.ds.filter.tip"));
		ftip.setEditable(false);
		ftip.setBackground(container.getBackground());
		ftip.setBounds((cwidth-100)*3/4-100,550,150,30);
		ftip.setForeground(new Color(46,139,87));
		ftip.setFont(new Font("华文新魏", Font.BOLD, 14));
		container.add(ftip);
		
		JButton button = new JButton(ResourceHolder.getProperty("dstt.btn.submit"));
		button.setForeground(Color.BLUE);
		button.setFont(new Font("华文楷体", Font.BOLD, 18));
		button.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				setVisible(false);
				handleData();
			}
		});
		button.setBounds(cwidth*7/8-50, cheight-50, 93, 30);
		container.add(button);
		JButton back = new JButton(ResourceHolder.getProperty("dstt.btn.back"));
		back.setForeground(Color.BLUE);
		back.setFont(new Font("华文楷体", Font.BOLD, 18));
		back.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				setVisible(false);
				new ManageTable();
			}
		});
		back.setBounds(cwidth*7/8+50, cheight-50, 93, 30);
		container.add(back);
		
		this.setVisible(true);
		this.setResizable(false);
		this.validate();
		this.repaint();
	}
	
	protected void handleData(){
		Component[] components = container.getComponents();
		if(components!=null&&components.length>0){
			DataInfo data = new DataInfo();
			Config source = data.new Config();
			Config target = data.new Config();
			for (Component component : components) {
				String name = component.getName();
				if(name!=null&&(name.endsWith("_source")||name.endsWith("_target"))){
					String value = null;
					if(component instanceof JTextField){
						JTextField obj = (JTextField)component;
						value = obj.getText();
					}
					if(component instanceof JComboBox){
						@SuppressWarnings("rawtypes")
						JComboBox obj = (JComboBox)component;
						value = (String)obj.getSelectedItem();
					}
					if(!StringUtil.isEmpty(value)){
						if(name.equalsIgnoreCase("servers_source")||name.equalsIgnoreCase("servers_target")){
							if(name.equalsIgnoreCase("servers_source")){
								source.setServers(value);
							}else{
								target.setServers(value);
							}
						}
						if(name.equalsIgnoreCase("type_source")||name.equalsIgnoreCase("type_target")){
							if(name.equalsIgnoreCase("type_source")){
								source.setType(handleType(value));
							}else{
								target.setType(handleType(value));
							}
						}
						if(name.equalsIgnoreCase("port_source")||name.equalsIgnoreCase("port_target")){
							if(name.equalsIgnoreCase("port_source")){
								source.setPort(Integer.valueOf(value));
							}else{
								target.setPort(Integer.valueOf(value));
							}
						}
						if(name.equalsIgnoreCase("version_source")||name.equalsIgnoreCase("version_target")){
							if(name.equalsIgnoreCase("version_source")){
								source.setVersion(value);
							}else{
								target.setVersion(value);
							}
						}
						if(name.equalsIgnoreCase("username_source")||name.equalsIgnoreCase("username_target")){
							if(name.equalsIgnoreCase("username_source")){
								source.setUsername(value);
							}else{
								target.setUsername(value);
							}
						}
						if(name.equalsIgnoreCase("password_source")||name.equalsIgnoreCase("password_target")){
							if(name.equalsIgnoreCase("password_source")){
								source.setPassword(value);
							}else{
								target.setPassword(value);
							}
						}
						if(name.equalsIgnoreCase("database_source")||name.equalsIgnoreCase("database_target")){
							if(name.equalsIgnoreCase("database_source")){
								source.setDatabase(value);
							}else{
								target.setDatabase(value);
							}
						}
						if(name.equalsIgnoreCase("schema_source")||name.equalsIgnoreCase("schema_target")){
							if(name.equalsIgnoreCase("schema_source")){
								source.setSchema(value);
							}else{
								target.setSchema(value);
							}
						}
						if(name.equalsIgnoreCase("keyspace_source")||name.equalsIgnoreCase("keyspace_target")){
							if(name.equalsIgnoreCase("keyspace_source")){
								source.setKeyspace(value);
							}else{
								target.setKeyspace(value);
							}
						}
						if(name.equalsIgnoreCase("table_source")||name.equalsIgnoreCase("table_target")){
							Map<String, String> mapping = data.getMapping();
							if(name.equalsIgnoreCase("table_source")){
								for(String table : value.split(",")){
									boolean flag = true;
									for(String key:mapping.keySet()){
										String ttable = mapping.get(key);
										if(!NumberUtil.isNumber(key)||ttable==null){
											continue;
										}
										for(String str:tableMatch(ttable)){//匹配目标库表
											if(table.contains(str)){
												flag = false;
												mapping.remove(key);
												mapping.put(table, ttable);
												break;
											}
										}
									}
									if(flag){
										mapping.put(table, null);//默认无匹配目标数据库
									}
								}
							}else{
								for(String table : value.split(",")){
									boolean flag = true;
									for(String key:mapping.keySet()){
										if(NumberUtil.isNumber(key)||mapping.get(key)!=null){
											continue;
										}
										for(String str:tableMatch(key)){//匹配源库表
											if(table.contains(str)){
												flag = false;
												mapping.put(key, table);
												break;
											}
										}
									}
									if(flag){
										mapping.put(new Random().nextInt()+"", table);//默认数字匹配源库表
									}
								}
							}
							for(String stable:mapping.keySet()){
								String ttable = mapping.get(stable);
								if(ttable==null){
									mapping.replace(stable, stable);
								}
								if(NumberUtil.isNumber(stable)){
									mapping.remove(stable);
//									mapping.put(ttable, ttable);
								}
							}
							data.setMapping(mapping);
						}
						if(name.equalsIgnoreCase("filter_source")||name.equalsIgnoreCase("filter_target")){
							List<String> filter_columns = Arrays.asList(value.split(","));
							data.setFilter_columns(filter_columns);
						}
					}
				}
			}
			data.setSource(source);
			data.setTarget(target);
			if(judge(data)){
				if(CURRENT_ROW>0){
					ManageTable.SYN_DATA.set(CURRENT_ROW, data);
				}else{
					ManageTable.SYN_DATA.add(data);
				}
				new ManageTable();
			}
		}
	}
	/**
	 * @decription 表名匹配
	 * @author yi.zhang
	 * @time 2017年7月27日 下午6:15:11
	 * @param table
	 * @return
	 */
	protected List<String> tableMatch(String table){
		List<String> result = new ArrayList<String>();
		result.add(table);
		for(int i=0;i<table.length();i++){
			char c = table.charAt(i);
			if(c=='_'||c=='-'||(c>='A'&&c<='Z')){
				if(c>='A'&&c<='Z'){
					result.add(table.substring(i));
				}else{
					result.add(table.substring(i+1));
					i++;
				}
			}
		}
		if(table.contains("_")||table.contains("-")){
			result.add(table.replaceAll("(_|-)", ""));
		}
		return result;
	}
	public boolean judge(DataInfo obj){
		Config source = obj.getSource();
		Config target = obj.getTarget();
		String sourceString = source.getServers()+","+source.getPort()+","+source.getType()+","+source.getDatabase()+","+source.getKeyspace()+","+source.getSchema();
		String targetString = target.getServers()+","+target.getPort()+","+target.getType()+","+target.getDatabase()+","+target.getKeyspace()+","+target.getSchema();
		if(StringUtil.isEmpty(source.getServers())||StringUtil.isEmpty(target.getServers())){
			return false;
		}
		if(sourceString.equals(targetString)&&StringUtils.join(obj.getMapping().keySet(), ",").equals(StringUtils.join(obj.getMapping().values(), ","))){
			return false;
		}
		return true;
	}
	public static void main(String[] args) {
		JFrame frame = new ConfigDialog();
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setVisible(true);
	}
}
