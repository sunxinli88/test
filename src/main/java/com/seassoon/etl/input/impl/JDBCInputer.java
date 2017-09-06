package com.seassoon.etl.input.impl;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.jfinal.plugin.activerecord.DbKit;
import com.seassoon.etl.input.AbstractInputer;
import com.seassoon.etl.input.Inputer;
import com.seassoon.utils.ConnectionDB;

public class JDBCInputer extends AbstractInputer implements Inputer {


	// public String table

	// public BlockingQueue<String[]> queue;

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

	private static Logger log = Logger.getLogger(JDBCInputer.class);

	private List<String> columnsList;

	public JDBCInputer(String sql, List<String> columnsList, Map<String, String> columnConvertMap, String URL,
			String USERNAME, String PASSWORD, String DRIVER) {

		super();
		this.sql = sql;
		// this.db = db;
		this.columnsList = columnsList;
		this.columnConvertMap = columnConvertMap;

		this.URL = URL;
		this.USERNAME = USERNAME;
		this.PASSWORD = PASSWORD;
		this.DRIVER = DRIVER;

		init();

	}

	public void init() {
		if (queue == null) {
			queue = new LinkedBlockingQueue<>(100000);
		}
		if (db == null) {
			db = new ConnectionDB(URL, USERNAME, PASSWORD, DRIVER);
		}

		initThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// 执行SQL获得结果集
				ResultSet rs = db.executeQueryRS(sql, null);

				ResultSetMetaData rsmd = null;

				// 结果集列数
				int columnCount = 0;
				try {
					rsmd = rs.getMetaData();

					// 获得结果集列数
					columnCount = rsmd.getColumnCount();
				} catch (SQLException e1) {
					System.out.println(e1.getMessage());
				}

				originalColumnsList = new ArrayList<>();
				try {
					for (int i = 1; i <= columnCount; i++) {

						originalColumnsList.add(rsmd.getColumnLabel(i));

					}

					columnConvert();

				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				Integer pageSize = 100000;
				List<Object> paramsList = new ArrayList<Object>();
				int currentCount = 0;
				String[] tempData = null;
				try {

					while (rs.next()) {
						currentCount++;

						if (columnsList == null || columnsList.size() == 0) {

							// columnsList = originalColumnsList;
							for (int i = 0; i < originalColumnsList.size(); i++) {
								columnsList.add(originalColumnsList.get(i));
							}
						}

						Map<String, Object> map = new HashMap<String, Object>();

						tempData = new String[columnsList.size()];

						for (int i = 0; i < originalColumnsList.size(); i++) {

							int columnIndex = columnsList.indexOf(originalColumnsList.get(i));
							if (columnIndex >= 0) {
								tempData[columnIndex] = rs.getObject(i + 1) == null ? null : rs.getString(i + 1);
							}

						}

						queue.put(tempData);

					}

				} catch (SQLException e) {
					System.out.println(e.getMessage());
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					// 关闭所有资源
					// closeAll();
					try {

						rs.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			}
		});
		initThread.start();
	}

}
