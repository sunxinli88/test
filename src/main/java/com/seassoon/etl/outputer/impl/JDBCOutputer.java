package com.seassoon.etl.outputer.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.seassoon.etl.outputer.AbstractOutPuter;
import com.seassoon.etl.outputer.OutPuter;
import com.seassoon.utils.ConnectionDB;

public class JDBCOutputer extends   AbstractOutPuter implements OutPuter {

	public String sql;

	public ConnectionDB db;

	/**
	 * 数据库驱动类名称
	 */
	private String DRIVER = "com.mysql.jdbc.Driver";

	/**
	 * 连接字符串
	 */
	private String URL = null;
	/**
	 * 用户名
	 */
	private String USERNAME = null;

	/**
	 * 密码
	 */
	private String PASSWORD = null;


	
	private static Logger log = Logger.getLogger(JDBCOutputer.class);




	private String tableName;

	private Integer submitSize = 100000;

	private List<String[]> list = new ArrayList<>();

	public Integer getSubmitSize() {
		return submitSize;
	}

	public void setSubmitSize(Integer submitSize) {
		this.submitSize = submitSize;
	}

	public JDBCOutputer( String tableName, List<String> columnsList) {

		this.columnsList = columnsList;

		this.tableName = tableName;

		init();

	}
	
	public JDBCOutputer(String tableName, List<String> columnsList, String URL,
			String USERNAME, String PASSWORD, String DRIVER) {

		super();
		this.tableName = tableName;
		// this.db = db;
		this.columnsList = columnsList;

		this.URL = URL;
		this.USERNAME = USERNAME;
		this.PASSWORD = PASSWORD;
		this.DRIVER = DRIVER;

		init();

	}

	private void init() {
		if (db == null) {
			db = new ConnectionDB(URL, USERNAME, PASSWORD, DRIVER);
		}

	}

	@Override
	public void process(String[] data) {

		if (data == null) {
			execute();
		} else {
			processNum.incrementAndGet();
			
			list.add(data);
			if (list.size() > submitSize) {
				execute();
			}

		}

	}

	public void execute() {
		List<Object> paramsList = new ArrayList<Object>();
		for (String[] temp : list) {
			for (String string : temp) {
				paramsList.add(string);
			}
			
		}
		db.insert(columnsList.toArray(new String[columnsList.size()]), paramsList.toArray(), tableName);
		list.clear();
		
		System.out.println(tableName+" output "+processNum);
	}

	@Override
	public void close() {
		this.execute();
//		try {
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
	}

}
