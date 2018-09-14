package com.share.swing;

import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.swing.AbstractCellEditor;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableColumnModel;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONObject;
import com.share.service.cassandra.CassandraFactory;
import com.share.service.elasticsearch.transport.ElasticsearchTransportFactory;
import com.share.service.greenplum.GreenplumFactory;
import com.share.service.jdbc.JDBCFactory;
import com.share.service.mongodb.MongoDBFactory;
import com.share.swing.DataInfo.Config;
import com.share.util.DateUtil;
import com.share.util.StringUtil;

public class ManageTable extends CommonFrame {
	private static final long serialVersionUID = 8927006591622687297L;
	/**
	 * 同步列表
	 */
	public static List<DataInfo> SYN_DATA= new ArrayList<DataInfo>();
	
	public ManageTable() {
		super._init();
		container = this.getContentPane();
		container.setForeground(Color.BLACK);
		container.setBackground(new Color(255, 255, 255));
		container.setFont(new Font("仿宋", Font.PLAIN, 12));
		container.setLayout(null);
		init();
	}
	
	protected void init(){
		container.removeAll();
		JTabbedPane jtab = new JTabbedPane(JTabbedPane.TOP);
		jtab.setFont(new Font("宋体", Font.BOLD, 14));
		jtab.setBounds(0, 0, width, height);
		JPanel canaltab = new JPanel();
		canaltab.setName("canaltab");
		jtab.addTab(ResourceHolder.getProperty("dstt.service.canal"), canaltab);
		jtab.setLayout(jtab.getLayout());
		JPanel syntab = new JPanel();
		syntab.setBackground(Color.WHITE);
		syntab.setName("syntab");
		syntab.setLayout(jtab.getLayout());
		jtab.addTab(ResourceHolder.getProperty("dstt.service.datalog"), syntab);
		JPanel logtab = new JPanel();
		logtab.setBackground(Color.WHITE);
		logtab.setName("logtab");
		logtab.setLayout(jtab.getLayout());
		jtab.addTab(ResourceHolder.getProperty("dstt.service.datasyn"), logtab);
		jtab.setSelectedComponent(syntab);
		jtab.addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent e) {
				JTabbedPane jtab = (JTabbedPane)e.getSource();
				JPanel panel = (JPanel)jtab.getSelectedComponent();
				if("canaltab".equalsIgnoreCase(panel.getName())){
					
				}if("logtab".equalsIgnoreCase(panel.getName())){
					
				}else{
					syn(panel);
				}
			}
		});
		container.add(jtab);
		syn(syntab);
	}
	
	public void syn(final JPanel default_panel){
		final ButtonGroup gbtn = new ButtonGroup();
		default_panel.removeAll();
		
		final JButton add = new JButton(ResourceHolder.getProperty("dstt.btn.add"));
		add.setFont(new Font("宋体", Font.BOLD, 14));
		add.setHorizontalAlignment(SwingConstants.CENTER);
		add.setCursor(new Cursor(Cursor.HAND_CURSOR));
		add.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				gbtn.clearSelection();
				JButton btn = (JButton)e.getSource();
				btn.setSelected(true);
				ConfigDialog.CURRENT_ROW=-1;
				ConfigDialog.CURRENT_DATA = new DataInfo();
				setVisible(false);
				new ConfigDialog();
			}
		});
		add.setBounds(50, 10, 100, 25);
		gbtn.add(add);
		default_panel.add(add);
		final JButton refresh = new JButton(ResourceHolder.getProperty("dstt.btn.refresh"));
		refresh.setFont(new Font("宋体", Font.BOLD, 14));
		refresh.setHorizontalAlignment(SwingConstants.CENTER);
		refresh.setCursor(new Cursor(Cursor.HAND_CURSOR));
		refresh.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				gbtn.clearSelection();
				JButton btn = (JButton)e.getSource();
				btn.setSelected(true);
				syn(default_panel);
			}
		});
		refresh.setBounds(150, 10, 100, 25);
		gbtn.add(refresh);
		default_panel.add(refresh);
		final JButton synall = new JButton(ResourceHolder.getProperty("dstt.btn.synall"));
		synall.setFont(new Font("宋体", Font.BOLD, 14));
		synall.setHorizontalAlignment(SwingConstants.CENTER);
		synall.setCursor(new Cursor(Cursor.HAND_CURSOR));
		synall.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				gbtn.clearSelection();
				JButton btn = (JButton)e.getSource();
				btn.setSelected(true);
			}
		});
		synall.setBounds(250, 10, 100, 25);
		gbtn.add(synall);
		default_panel.add(synall);
		
		Object[][] hdata = new Object[0][2];
		String[] hcolumns = {ResourceHolder.getProperty("dstt.ds.source"), ResourceHolder.getProperty("dstt.ds.target")};
		JTable table = new JTable(hdata, hcolumns);
		JTableHeader header = table.getTableHeader();
		header.setReorderingAllowed(false);//表格列不可移动 
		header.setResizingAllowed(false);
		DefaultTableCellRenderer  renderer = (DefaultTableCellRenderer) header.getDefaultRenderer();  
		renderer.setHorizontalAlignment(DefaultTableCellRenderer.CENTER);//列名居中 
        header.setDefaultRenderer(renderer);
        header.setForeground(Color.BLUE);
        header.setBackground(new Color(	238,232,170));
        header.setFont(new Font("楷体", Font.BOLD, 18));
        
        int cwidth = container.getSize().width;
        cwidth=cwidth>0?cwidth:width;
        int cheight = container.getSize().height;
        cheight=cheight>0?cheight:height;
		JScrollPane scroll = new JScrollPane(table);//滚动条
		scroll.setBounds(50, 35, cwidth-100, 27);
		default_panel.add(scroll);
		
		String[] tcolumns = {ResourceHolder.getProperty("dstt.ds.seq"), ResourceHolder.getProperty("dstt.ds.saddress"), ResourceHolder.getProperty("dstt.ds.stype"), ResourceHolder.getProperty("dstt.ds.taddress"),ResourceHolder.getProperty("dstt.ds.ttype"),ResourceHolder.getProperty("dstt.ds.action")};
		Object[][] data= handleData();
//		JTable dtable = new JTable(data, tcolumns);
		JTable dtable = new JTable();
		dtable.setRowHeight(40);
		JTableHeader dheader = dtable.getTableHeader();
		dheader.setReorderingAllowed(false);//表格列不可移动 
		dheader.setResizingAllowed(false);
		DefaultTableCellRenderer  drenderer = (DefaultTableCellRenderer) dheader.getDefaultRenderer();  
		drenderer.setHorizontalAlignment(DefaultTableCellRenderer.CENTER);//列名居中 
		drenderer.setVerticalAlignment(DefaultTableCellRenderer.CENTER);
        dheader.setDefaultRenderer(renderer);
        dheader.setForeground(Color.RED);
        dheader.setBackground(new Color(220,245,255));
        dheader.setFont(new Font("Default", Font.BOLD, 14));
		JScrollPane scroll1 = new JScrollPane(dtable);//滚动条
		scroll1.setBounds(50, 60, cwidth-100, cheight-2*60);
		default_panel.add(scroll1);
		DefaultTableModel dmodel = (DefaultTableModel) dtable.getModel();
		dmodel.setColumnIdentifiers(tcolumns);
		for (int i=0;i<data.length;i++) {
			dmodel.addRow(data[i]);
		}
		
		DefaultTableColumnModel cmodel = (DefaultTableColumnModel)dtable.getColumnModel();
		int cnum = cmodel.getColumnCount();
		for(int i=0;i<cnum;i++){
			TableColumn column = cmodel.getColumn(i);
			DefaultTableCellRenderer  crenderer = new DefaultTableCellRenderer();
			crenderer.setHorizontalAlignment(DefaultTableCellRenderer.CENTER);//列名居中
			crenderer.setVerticalAlignment(DefaultTableCellRenderer.CENTER);
			if(i%2==0){
				if(i==0){
					column.setMaxWidth(80);
					crenderer.setForeground(Color.BLUE);
				}else{
					column.setWidth(scroll1.getWidth()/cnum);
				}
			}
			column.setCellRenderer(crenderer);
			if(i==cnum-1){
				JTableButtonRenderer tablebutton = new JTableButtonRenderer(default_panel);
				column.setCellRenderer(tablebutton);
				column.setCellEditor(tablebutton);
			}
		}
//		dtable.invalidate();
		this.setVisible(true);
		this.setResizable(false);
		this.validate();
		this.repaint();
	}
	
	protected Object[][] handleData(){
		Object[][] data=new Object[SYN_DATA.size()][];
		for (DataInfo obj : SYN_DATA) {
			int i = SYN_DATA.indexOf(obj);
			Config source = obj.getSource();
			Config target = obj.getTarget();
			String sname = "";
			String stype = "";
			String tname = "";
			String ttype = "";
			if(source!=null){
				sname = handleServers(source.getServers(),source.getPort());
				stype = handleType(source.getType());
			}
			if(target!=null){
				tname = handleServers(target.getServers(),target.getPort());
				ttype = handleType(target.getType());
			}
			data[i]=new Object[]{i+1,sname,stype,tname,ttype,null};
		}
		return data;
	}
	
	public class JTableButtonRenderer extends AbstractCellEditor implements TableCellRenderer, TableCellEditor {

		private static final long serialVersionUID = 3740170279977660631L;
		JPanel panel = new JPanel();
		private int default_row = -1;
		public JTableButtonRenderer(final JPanel default_panel){
			JButton update = new JButton(ResourceHolder.getProperty("dstt.btn.update"));
			update.setFont(new Font("宋体", Font.BOLD, 14));
			update.setHorizontalAlignment(SwingConstants.CENTER);
			update.setCursor(new Cursor(Cursor.HAND_CURSOR));
			update.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					ConfigDialog.CURRENT_ROW=default_row;
					ConfigDialog.CURRENT_DATA = SYN_DATA.get(default_row);
					setVisible(false);
					new ConfigDialog();
				}
			});
			panel.add(update);
			JButton syn = new JButton(ResourceHolder.getProperty("dstt.btn.syn"));
			syn.setFont(new Font("宋体", Font.BOLD, 14));
			syn.setHorizontalAlignment(SwingConstants.CENTER);
			syn.setCursor(new Cursor(Cursor.HAND_CURSOR));
			syn.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					DataInfo data = SYN_DATA.get(default_row);
					handleSyn(data);
				}
			});
			panel.add(syn);
			JButton remove = new JButton(ResourceHolder.getProperty("dstt.btn.remove"));
			remove.setFont(new Font("宋体", Font.BOLD, 14));
			remove.setHorizontalAlignment(SwingConstants.CENTER);
			remove.setCursor(new Cursor(Cursor.HAND_CURSOR));
			remove.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
//					System.out.println("remove:"+SYN_DATA.size()+"-->"+default_row+"-->"+dtable.getRowCount());
					if(default_row<SYN_DATA.size()){
						SYN_DATA.remove(default_row);
					}
					syn(default_panel);
				}
			});
			panel.add(remove);
		}

		@Override
		public Object getCellEditorValue() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected, int row, int column) {
//			System.out.println("--1---value:"+value+",isSelected:"+isSelected+",row:"+row+",column:"+column);
			table.clearSelection();
			default_row = row;
			return panel;
		}

		@Override
		public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus,int row, int column) {
//			System.out.println("--2---value:"+value+",isSelected:"+isSelected+",hasFocus:"+hasFocus+",row:"+row+",column:"+column);
			table.clearSelection();
			default_row = row;
			return panel;
		}
	}
	/**
	 * @decription 数据同步
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:32:39
	 * @param obj	数据配置
	 */
	public void handleSyn(DataInfo obj){
		if(obj==null){
			return;
		}
		Config source = obj.getSource();
		Config target = obj.getTarget();
		if(source==null||target==null){
			return;
		}
		if(!StringUtil.isEmpty(source.getServers())&&!StringUtil.isEmpty(target.getServers())){
			double stype = source.getType();
			double ttype = target.getType();
			List<String> filter_columns = obj.getFilter_columns();
			if(stype==2.1){//Cassandra
				if(ttype==2.1){//Cassandra
					cassandra2cassandra(source, target, obj.getMapping(), filter_columns);
				}
				if(ttype==2.2){//MongoDB
					cassandra2mongodb(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==3){//MySQL | SQL　Server | Oracle
					cassandra2sql(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==4){//Greenplum
					cassandra2greenplum(source, target, obj.getMapping(), filter_columns);
				}
				if(ttype==1){//Eleasticsearch
//					cassandra2eleasticsearch(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==5){//Kafka
//					cassandra2kafka(source, target, obj.getMapping(), filter_columns);
				}
			}
			if(stype==2.2){//MongoDB
				if(ttype==2.1){//Cassandra
					mongodb2cassandra(source, target, obj.getMapping(), filter_columns);
				}
				if(ttype==2.2){//MongoDB
					mongodb2mongodb(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==3){//MySQL | SQL　Server | Oracle
					mongodb2sql(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==4){//Greenplum
					mongodb2greenplum(source, target, obj.getMapping(), filter_columns);
				}
				if(ttype==1){//Eleasticsearch
					mongodb2eleasticsearch(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==5){//Kafka
//					mongodb2kafka(source, target, obj.getMapping(), filter_columns);
				}
			}
			if(Double.valueOf(stype).intValue()==3){//MySQL | SQL　Server | Oracle
				if(ttype==2.1){//Cassandra
					sql2cassandra(source, target, obj.getMapping(), filter_columns);
				}
				if(ttype==2.2){//MongoDB
					sql2mongodb(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==3){//MySQL | SQL　Server | Oracle
					sql2sql(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==4){//Greenplum
					sql2greenplum(source, target, obj.getMapping(), filter_columns);
				}
				if(ttype==1){//Eleasticsearch
//					sql2eleasticsearch(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==5){//Kafka
//					sql2kafka(source, target, obj.getMapping(), filter_columns);
				}
			}
			if(stype==4.1){//Greenplum
				if(ttype==2.1){//Cassandra
					greenplum2cassandra(source, target, obj.getMapping(), filter_columns);
				}
				if(ttype==2.2){//MongoDB
					greenplum2mongodb(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==3){//MySQL | SQL　Server | Oracle
					greenplum2sql(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==4){//Greenplum
					greenplum2greenplum(source, target, obj.getMapping(), filter_columns);
				}
				if(ttype==1){//Eleasticsearch
//					greenplum2eleasticsearch(source, target, obj.getMapping(), filter_columns);
				}
				if(Double.valueOf(ttype).intValue()==5){//Kafka
//					greenplum2kafka(source, target, obj.getMapping(), filter_columns);
				}
			}
		}
	}
	/**
	 * @decription 数据同步(Cassandra-->Cassandra)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void cassandra2cassandra(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		CassandraFactory factory = new CassandraFactory();
		factory.init(source.getServers(), source.getKeyspace(), source.getUsername(), source.getPassword());
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		CassandraFactory tfactory = new CassandraFactory();
		tfactory.init(target.getServers(), target.getKeyspace(), target.getUsername(), target.getPassword());
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
//			if(!(stables.contains(stable)&&ttables.contains(ttable))){
//				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
//				continue;
//			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String cql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(cql, null);
			System.out.println("--目标表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
//				tfactory.save(tdata);
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	/**
	 * @decription 数据同步(Cassandra-->MongoDB)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void cassandra2mongodb(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		CassandraFactory factory = new CassandraFactory();
		factory.init(source.getServers(), source.getKeyspace(), source.getUsername(), source.getPassword());
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		MongoDBFactory tfactory = new MongoDBFactory();
		tfactory.init(target.getServers(), target.getDatabase(), target.getSchema(), target.getUsername(), target.getPassword());
//		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(ttables.contains(ttable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String cql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(cql, null);
			System.out.println("--目标表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(!key.matches("(\\w+)")){
						continue;
					}
					tdata.replace(reflect.get(key), value);
				}
				tfactory.save(ttable, tdata);
			}
		}
	}
	/**
	 * @decription 数据同步(Cassandra-->MySQL|SQL　Server|Oracle)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void cassandra2sql(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		CassandraFactory factory = new CassandraFactory();
		factory.init(source.getServers(), source.getKeyspace(), source.getUsername(), source.getPassword());
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		JDBCFactory tfactory = new JDBCFactory();
		String driverName=null,url = null;
		if(target.getType()==3.1){//MySQL
			driverName="com.mysql.jdbc.Driver";
			url = "jdbc:mysql://"+target.getServers()+(target.getPort()>0?":"+target.getPort():"")+"/"+target.getDatabase()+"?useUnicode=true&characterEncoding=UTF8";
		}
		if(target.getType()==3.2){//SQL　Server
			driverName="com.microsoft.jdbc.sqlserver.SQLServerDriver";
			url = "jdbc:microsoft://"+target.getServers()+(target.getPort()>0?":"+target.getPort():"")+";database="+target.getDatabase()+";encrypt=true;trustServerCertificate=false;useUnicode=true;characterEncoding=UTF8";
		}
		if(target.getType()==3.3){//Oracle
			driverName="oracle.jdbc.driver.OracleDriver";
			url = "jdbc:oracle:thin:@"+target.getServers()+(target.getPort()>0?":"+target.getPort():"")+":"+target.getDatabase();
		}
		tfactory.init(driverName, url,target.getUsername(), target.getPassword(), true, 100, 10);
//		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(ttables.contains(ttable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String cql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(cql, null);
			System.out.println("--目标表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
//				tfactory.save(tdata);
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	/**
	 * @decription 数据同步(Cassandra-->Greenplum)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void cassandra2greenplum(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		CassandraFactory factory = new CassandraFactory();
		factory.init(source.getServers(), source.getKeyspace(), source.getUsername(), source.getPassword());
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		GreenplumFactory tfactory = new GreenplumFactory();
		tfactory.init(target.getServers(), target.getDatabase(), target.getSchema(), target.getUsername(), target.getPassword(), true, 100, 10);
//		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(ttables.contains(ttable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String cql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(cql, null);
			System.out.println("--目标表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
//				tfactory.save(tdata);
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	/**
	 * @decription 数据同步(MongoDB-->Cassandra)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void mongodb2cassandra(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		MongoDBFactory factory = new MongoDBFactory();
		factory.init(source.getServers(), source.getDatabase(), source.getSchema(), source.getUsername(), source.getPassword());
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		CassandraFactory tfactory = new CassandraFactory();
		tfactory.init(target.getServers(), target.getKeyspace(), target.getUsername(), target.getPassword());
		List<String> stables = factory.queryTables();
//		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			tfactory.queryColumns(ttable);
			if(!(stables.contains(stable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			List<?> datas = factory.executeQuery(stable, null, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	/**
	 * @decription 数据同步(MongoDB-->MySQL|SQL　Server|Oracle)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void mongodb2sql(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		MongoDBFactory factory = new MongoDBFactory();
		factory.init(source.getServers(), source.getDatabase(), source.getSchema(), source.getUsername(), source.getPassword());
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		JDBCFactory tfactory = new JDBCFactory();
		String driverName=null,url = null;
		if(target.getType()==3.1){//MySQL
			driverName="com.mysql.jdbc.Driver";
			url = "jdbc:mysql://"+target.getServers()+(target.getPort()>0?":"+target.getPort():"")+"/"+target.getDatabase()+"?useUnicode=true&characterEncoding=UTF8";
		}
		if(target.getType()==3.2){//SQL　Server
			driverName="com.microsoft.jdbc.sqlserver.SQLServerDriver";
			url = "jdbc:microsoft://"+target.getServers()+(target.getPort()>0?":"+target.getPort():"")+";database="+target.getDatabase()+";encrypt=true;trustServerCertificate=false;useUnicode=true;characterEncoding=UTF8";
		}
		if(target.getType()==3.3){//Oracle
			driverName="oracle.jdbc.driver.OracleDriver";
			url = "jdbc:oracle:thin:@"+target.getServers()+(target.getPort()>0?":"+target.getPort():"")+":"+target.getDatabase();
		}
		tfactory.init(driverName, url,target.getUsername(), target.getPassword(), true, 100, 10);
		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(stables.contains(stable)&&ttables.contains(ttable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			List<?> datas = factory.executeQuery(stable, null, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	/**
	 * @decription 数据同步(MongoDB-->Greenplum)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void mongodb2greenplum(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		MongoDBFactory factory = new MongoDBFactory();
		factory.init(source.getServers(), source.getDatabase(), source.getSchema(), source.getUsername(), source.getPassword());
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		GreenplumFactory tfactory = new GreenplumFactory();
		tfactory.init(target.getServers(), target.getDatabase(), target.getSchema(), target.getUsername(), target.getPassword(), true, 100, 10);
		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(stables.contains(stable)&&ttables.contains(ttable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			List<?> datas = factory.executeQuery(stable, null, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	/**
	 * @decription 数据同步(MongoDB-->MongoDB)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void mongodb2mongodb(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		MongoDBFactory factory = new MongoDBFactory();
		factory.init(source.getServers(), source.getDatabase(), source.getSchema(), source.getUsername(), source.getPassword());
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		MongoDBFactory tfactory = new MongoDBFactory();
		tfactory.init(target.getServers(), target.getDatabase(), target.getSchema(), target.getUsername(), target.getPassword());
		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!stables.contains(stable)){
				System.out.println("--数据表["+stable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = ttables!=null&&ttables.contains(ttable)?tfactory.queryColumns(ttable):scolumns;
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			List<?> datas = factory.executeQuery(stable, null, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(!key.matches("(\\w+)")){
						continue;
					}
					tdata.replace(reflect.get(key), value);
				}
				tfactory.save(ttable, tdata);
			}
		}
	}
	/**
	 * @decription 数据同步(MongoDB-->Eleasticsearch)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void mongodb2eleasticsearch(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		MongoDBFactory factory = new MongoDBFactory();
		factory.init(source.getServers(), source.getDatabase(), source.getSchema(), source.getUsername(), source.getPassword());
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		ElasticsearchTransportFactory tfactory = new ElasticsearchTransportFactory(target.getDatabase(), target.getServers(), target.getUsername(), target.getPassword());
		tfactory.init();
		List<String> stables = factory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!stables.contains(stable)){
				System.out.println("--数据表["+stable+"]不存在--");
				continue;
			}
//			Map<String,String> reflect = new LinkedHashMap<String,String>();
//			Map<String, String> scolumns = factory.queryColumns(stable);
//			Map<String, String> tcolumns = ttables!=null&&ttables.contains(ttable)?tfactory.queryColumns(ttable):scolumns;
//			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
//				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
//				continue;
//			}
//			for(String scolumn:scolumns.keySet()){
//				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
//				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
//					continue;
//				}
//				for(String tcolumn:tcolumns.keySet()){
//					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
//					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
//						continue;
//					}
//					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
//						reflect.put(scolumn, tcolumn);
//					}
//				}
//			}
//			if(reflect.isEmpty()){
//				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
//				continue;
//			}
			List<?> datas = factory.executeQuery(stable, null, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				JSONObject json = (JSONObject)data;
				tfactory.upsert(source.getSchema(), ttable, json.getString("_id"), json.toJSONString());
			}
		}
	}
	/**
	 * @decription 数据同步([MySQL|SQL　Server|Oracle]-->[Cassandra])
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void sql2cassandra(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		JDBCFactory factory = new JDBCFactory();
		String driverName=null,url = null;
		if(source.getType()==3.1){//MySQL
			driverName="com.mysql.jdbc.Driver";
			url = "jdbc:mysql://"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+"/"+source.getDatabase()+"?useUnicode=true&characterEncoding=UTF8";
		}
		if(source.getType()==3.2){//SQL　Server
			driverName="com.microsoft.jdbc.sqlserver.SQLServerDriver";
			url = "jdbc:microsoft://"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+";database="+source.getDatabase()+";encrypt=true;trustServerCertificate=false;useUnicode=true;characterEncoding=UTF8";
		}
		if(source.getType()==3.3){//Oracle
			driverName="oracle.jdbc.driver.OracleDriver";
			url = "jdbc:oracle:thin:@"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+":"+source.getDatabase();
		}
		factory.init(driverName, url,source.getUsername(), source.getPassword(), true, 100, 10);
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		CassandraFactory tfactory = new CassandraFactory();
		tfactory.init(target.getServers(), target.getKeyspace(), target.getUsername(), target.getPassword());
		List<String> stables = factory.queryTables();
//		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(stables.contains(stable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String ssql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(ssql, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	/**
	 * @decription 数据同步([MySQL|SQL　Server|Oracle]-->[MongoDB])
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void sql2mongodb(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		JDBCFactory factory = new JDBCFactory();
		String driverName=null,url = null;
		if(source.getType()==3.1){//MySQL
			driverName="com.mysql.jdbc.Driver";
			url = "jdbc:mysql://"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+"/"+source.getDatabase()+"?useUnicode=true&characterEncoding=UTF8";
		}
		if(source.getType()==3.2){//SQL　Server
			driverName="com.microsoft.jdbc.sqlserver.SQLServerDriver";
			url = "jdbc:microsoft://"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+";database="+source.getDatabase()+";encrypt=true;trustServerCertificate=false;useUnicode=true;characterEncoding=UTF8";
		}
		if(source.getType()==3.3){//Oracle
			driverName="oracle.jdbc.driver.OracleDriver";
			url = "jdbc:oracle:thin:@"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+":"+source.getDatabase();
		}
		factory.init(driverName, url,source.getUsername(), source.getPassword(), true, 100, 10);
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		MongoDBFactory tfactory = new MongoDBFactory();
		tfactory.init(target.getServers(), target.getDatabase(), target.getSchema(), target.getUsername(), target.getPassword());
		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(stables.contains(stable)&&ttables.contains(ttable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String ssql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(ssql, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(!key.matches("(\\w+)")){
						continue;
					}
					tdata.replace(reflect.get(key), value);
				}
				tfactory.save(ttable, tdata);
			}
		}
	}
	/**
	 * @decription 数据同步([MySQL|SQL　Server|Oracle]-->[MySQL|SQL　Server|Oracle])
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void sql2sql(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		JDBCFactory factory = new JDBCFactory();
		String sdriverName=null;
		String surl = null;
		if(source.getType()==3.1){//MySQL
			sdriverName="com.mysql.jdbc.Driver";
			surl = "jdbc:mysql://"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+"/"+source.getDatabase()+"?useUnicode=true&characterEncoding=UTF8";
		}
		if(source.getType()==3.2){//SQL　Server
			sdriverName="com.microsoft.jdbc.sqlserver.SQLServerDriver";
			surl = "jdbc:microsoft://"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+";database="+source.getDatabase()+";encrypt=true;trustServerCertificate=false;useUnicode=true;characterEncoding=UTF8";
		}
		if(source.getType()==3.3){//Oracle
			sdriverName="oracle.jdbc.driver.OracleDriver";
			surl = "jdbc:oracle:thin:@"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+":"+source.getDatabase();
		}
		factory.init(sdriverName, surl,source.getUsername(), source.getPassword(), true, 100, 10);
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		JDBCFactory tfactory = new JDBCFactory();
		String tdriverName="";
		String turl = "";
		if(source.getType()==3.1){//MySQL
			tdriverName="com.mysql.jdbc.Driver";
			turl = "jdbc:mysql://"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+"/"+source.getDatabase()+"?useUnicode=true&characterEncoding=UTF8";
		}
		if(source.getType()==3.2){//SQL　Server
			tdriverName="com.microsoft.jdbc.sqlserver.SQLServerDriver";
			turl = "jdbc:microsoft://"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+";database="+source.getDatabase()+";encrypt=true;trustServerCertificate=false;useUnicode=true;characterEncoding=UTF8";
		}
		if(source.getType()==3.3){//Oracle
			tdriverName="oracle.jdbc.driver.OracleDriver";
			turl = "jdbc:oracle:thin:@"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+":"+source.getDatabase();
		}
		tfactory.init(tdriverName, turl,target.getUsername(), target.getPassword(), true, 100, 10);
		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(stables.contains(stable)&&ttables.contains(ttable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String ssql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(ssql, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	/**
	 * @decription 数据同步([MySQL|SQL　Server|Oracle]-->[Greenplum])
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void sql2greenplum(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		JDBCFactory factory = new JDBCFactory();
		String driverName=null,url = null;
		if(source.getType()==3.1){//MySQL
			driverName="com.mysql.jdbc.Driver";
			url = "jdbc:mysql://"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+"/"+source.getDatabase()+"?useUnicode=true&characterEncoding=UTF8";
		}
		if(source.getType()==3.2){//SQL　Server
			driverName="com.microsoft.jdbc.sqlserver.SQLServerDriver";
			url = "jdbc:microsoft://"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+";database="+source.getDatabase()+";encrypt=true;trustServerCertificate=false;useUnicode=true;characterEncoding=UTF8";
		}
		if(source.getType()==3.3){//Oracle
			driverName="oracle.jdbc.driver.OracleDriver";
			url = "jdbc:oracle:thin:@"+source.getServers()+(source.getPort()>0?":"+source.getPort():"")+":"+source.getDatabase();
		}
		factory.init(driverName, url,source.getUsername(), source.getPassword(), true, 100, 10);
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		GreenplumFactory tfactory = new GreenplumFactory();
		tfactory.init(target.getServers(), target.getDatabase(), target.getSchema(), target.getUsername(), target.getPassword(), true, 100, 10);
		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(stables.contains(stable)&&ttables.contains(ttable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String ssql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(ssql, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	/**
	 * @decription 数据同步(Greenplum-->Cassandra)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void greenplum2cassandra(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		GreenplumFactory factory = new GreenplumFactory();
		factory.init(source.getServers(), source.getDatabase(), source.getSchema(), source.getUsername(), source.getPassword(), true, 100, 10);
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		CassandraFactory tfactory = new CassandraFactory();
		tfactory.init(target.getServers(), target.getKeyspace(), target.getUsername(), target.getPassword());
		List<String> stables = factory.queryTables();
//		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(stables.contains(stable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String ssql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(ssql, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	/**
	 * @decription 数据同步(Greenplum-->MongoDB)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void greenplum2mongodb(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		GreenplumFactory factory = new GreenplumFactory();
		factory.init(source.getServers(), source.getDatabase(), source.getSchema(), source.getUsername(), source.getPassword(), true, 100, 10);
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		MongoDBFactory tfactory = new MongoDBFactory();
		tfactory.init(target.getServers(), target.getDatabase(), target.getSchema(), target.getUsername(), target.getPassword());
		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(stables.contains(stable)&&ttables.contains(ttable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String ssql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(ssql, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(!key.matches("(\\w+)")){
						continue;
					}
					tdata.replace(reflect.get(key), value);
				}
				tfactory.save(ttable, tdata);
			}
		}
	}
	/**
	 * @decription 数据同步(Greenplum-->Greenplum)
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void greenplum2greenplum(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		GreenplumFactory factory = new GreenplumFactory();
		factory.init(source.getServers(), source.getDatabase(), source.getSchema(), source.getUsername(), source.getPassword(), true, 100, 10);
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		GreenplumFactory tfactory = new GreenplumFactory();
		tfactory.init(target.getServers(), target.getDatabase(), target.getSchema(), target.getUsername(), target.getPassword(), true, 100, 10);
		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(stables.contains(stable)&&ttables.contains(ttable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String ssql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(ssql, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	/**
	 * @decription 数据同步(Greenplum-->[MySQL|SQL　Server|Oracle])
	 * @author yi.zhang
	 * @time 2017年8月4日 下午5:26:59
	 * @param source	数据源
	 * @param target	目标库
	 * @param mapper	表映射
	 * @param filter_columns	字段过滤
	 */
	protected void greenplum2sql(Config source,Config target,Map<String,String> mapper,List<String> filter_columns){
		if(source==null||target==null){
			return;
		}
		GreenplumFactory factory = new GreenplumFactory();
		factory.init(source.getServers(), source.getDatabase(), source.getSchema(), source.getUsername(), source.getPassword(), true, 100, 10);
		
		JDBCFactory tfactory = new JDBCFactory();
		String driverName=null,url = null;
		if(target.getType()==3.1){//MySQL
			driverName="com.mysql.jdbc.Driver";
			url = "jdbc:mysql://"+target.getServers()+(target.getPort()>0?":"+target.getPort():"")+"/"+target.getDatabase()+"?useUnicode=true&characterEncoding=UTF8";
		}
		if(target.getType()==3.2){//SQL　Server
			driverName="com.microsoft.jdbc.sqlserver.SQLServerDriver";
			url = "jdbc:microsoft://"+target.getServers()+(target.getPort()>0?":"+target.getPort():"")+";database="+target.getDatabase()+";encrypt=true;trustServerCertificate=false;useUnicode=true;characterEncoding=UTF8";
		}
		if(target.getType()==3.3){//Oracle
			driverName="oracle.jdbc.driver.OracleDriver";
			url = "jdbc:oracle:thin:@"+target.getServers()+(target.getPort()>0?":"+target.getPort():"")+":"+target.getDatabase();
		}
		tfactory.init(driverName, url,target.getUsername(), target.getPassword(), true, 100, 10);
		Map<String,String> mapping = new HashMap<String,String>();
		if(mapper==null||mapper.size()==0){
			List<String> tables = factory.queryTables();
			for (String table : tables) {
				mapping.put(table, table);
			}
		}else{
			mapping = mapper;
		}
		List<String> stables = factory.queryTables();
		List<String> ttables = tfactory.queryTables();
		for(String stable : mapping.keySet()){
			String ttable = mapping.get(stable);
			if(!(stables.contains(stable)&&ttables.contains(ttable))){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]不存在--");
				continue;
			}
			Map<String,String> reflect = new LinkedHashMap<String,String>();
			Map<String, String> scolumns = factory.queryColumns(stable);
			Map<String, String> tcolumns = tfactory.queryColumns(ttable);
			if(scolumns==null||scolumns.isEmpty()||tcolumns==null||tcolumns.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无合适字段--");
				continue;
			}
			for(String scolumn:scolumns.keySet()){
				String s_column = scolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
				if(filter_columns!=null&&(filter_columns.contains(scolumn)||filter_columns.contains(s_column))){
					continue;
				}
				for(String tcolumn:tcolumns.keySet()){
					String t_column = tcolumn.trim().toLowerCase().replaceAll("(_+?|-+?)", "");
					if(filter_columns!=null&&(filter_columns.contains(tcolumn)||filter_columns.contains(t_column))){
						continue;
					}
					if(scolumn.equalsIgnoreCase(tcolumn)||scolumn.equalsIgnoreCase(t_column)||s_column.equalsIgnoreCase(tcolumn)||s_column.equalsIgnoreCase(t_column)){
						reflect.put(scolumn, tcolumn);
					}
				}
			}
			if(reflect.isEmpty()){
				System.out.println("--数据表["+stable+"]或目标表["+ttable+"]无对应字段--");
				continue;
			}
			String ssql = "select "+StringUtils.join(reflect.keySet(), ",")+" from "+stable;
			List<?> datas = factory.executeQuery(ssql, null);
			System.out.println("--数据表["+stable+"]数据量:"+datas.size());
			for (Object data : datas) {
				Map<String,Object> tdata = new LinkedHashMap<String,Object>();
				JSONObject json = (JSONObject)data;
				for(String key:json.keySet()){
					Object value = json.get(key);
					if(value instanceof Date){
						value = DateUtil.formatDateTimeStr((Date)value);
					}
					if(value instanceof String){
						value = "\""+json.getString(key)+"\"";
					}
					tdata.replace(reflect.get(key), value);
				}
				String sql = "insert into "+ttable+"("+StringUtils.join(tdata.keySet(), ",")+")values("+StringUtils.join(tdata.values(), ",")+")";
				tfactory.executeUpdate(sql);
			}
		}
	}
	public static void main(String[] args) {
		JFrame frame = new ManageTable();
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setVisible(true);
	}
}
