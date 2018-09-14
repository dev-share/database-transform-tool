package com.share.swing;

import java.awt.Color;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Locale;

import javax.swing.ButtonGroup;
import javax.swing.DropMode;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JRadioButton;
import javax.swing.JTextPane;
import javax.swing.SwingConstants;

public class Main extends CommonFrame {
	private static final long serialVersionUID = 8927006591622687297L;

	public Main() {
		super._init();
		language(Locale.CHINESE);
	}
	
	protected void init(){
		this.setTitle("®™"+title+" "+ version +" ©ZhangYi");
		container = this.getContentPane();
		container.setForeground(Color.BLACK);
		container.setBackground(new Color(204, 255, 255));
		container.setFont(new Font("仿宋", Font.PLAIN, 12));
		container.setLayout(null);
		int cwidth = container.getSize().width>0?container.getSize().width:width;
        int cheight = container.getSize().height>0?container.getSize().height:height;
		
		JLabel jtitle = new JLabel(title);
		jtitle.setLabelFor(getContentPane());
		jtitle.setToolTipText(decription);
		jtitle.setHorizontalAlignment(SwingConstants.CENTER);
		jtitle.setForeground(Color.RED);
		jtitle.setBackground(Color.WHITE);
		//为标签设置及添加到框架
		jtitle.setBounds(cwidth*3/8,98,cwidth/4,81);
		jtitle.setFont(new Font("华文新魏", Font.BOLD, 30));
		container.add(jtitle);
		
		JTextPane textPane = new JTextPane();
		textPane.setDropMode(DropMode.USE_SELECTION);
		textPane.setForeground(Color.BLACK);
		textPane.setBackground(new Color(245, 255, 250));
		textPane.setEditable(false);
		textPane.setFont(new Font("华文宋体", Font.BOLD, 16));
		textPane.setText(decription.replace("\n", "\n\n"));
		textPane.setBounds(cwidth*5/16, 200, cwidth*3/8, cheight*3/8);
		container.add(textPane);
		
		JButton button = new JButton(ResourceHolder.getProperty("dstt.btn.next"));
		button.setFont(new Font("宋体", Font.BOLD, 14));
		button.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				setVisible(false);
				new ManageTable();
			}
		});
		button.setBounds(cwidth*7/8, cheight*15/16, 93, 30);
		container.add(button);
		
		final ButtonGroup language = new ButtonGroup();
		
		final JRadioButton chinese = new JRadioButton(ResourceHolder.getProperty("system.language.zh_CN"));
		chinese.setBackground(new Color(224, 255, 255));
		chinese.setHorizontalAlignment(SwingConstants.CENTER);
		chinese.setFont(new Font("仿宋", Font.BOLD, 12));
		chinese.setBounds(cwidth*14/16, 16, 75, 23);
		language.add(chinese);
		final JRadioButton english = new JRadioButton(ResourceHolder.getProperty("system.language.en_US"));
		english.setBackground(new Color(224, 255, 255));
		english.setHorizontalAlignment(SwingConstants.CENTER);
		english.setFont(new Font("仿宋", Font.BOLD, 12));
		english.setBounds(cwidth*15/16, 16, 75, 23);
		language.add(english);
		chinese.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				language.clearSelection();
				language(Locale.CHINESE);
			}
		});
		english.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				language.clearSelection();
				language(Locale.ENGLISH);
			}
		});
		if(this.getLocale().equals(Locale.ENGLISH)){
			english.setSelected(true);
		}else{
			chinese.setSelected(true);
		}
		container.add(chinese);
		container.add(english);
	}
	
	public void language(Locale locale){
		if(locale==null)locale=Locale.getDefault();
		ResourceHolder.locale=locale;
		this.setLocale(locale);
		title = ResourceHolder.getProperty("system.name");
		version = ResourceHolder.getProperty("system.version");
		decription = ResourceHolder.getProperty("system.decription");
		this.getContentPane().removeAll();
		init();
		
		this.setVisible(true);
		this.setResizable(false);
		this.validate();
		this.repaint();
	}
	public static void main(String[] args) {
		JFrame frame = new Main();
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setVisible(true);
	}
}
