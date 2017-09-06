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
import org.bson.Document;

import com.jfinal.plugin.activerecord.DbKit;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.seassoon.etl.input.AbstractInputer;
import com.seassoon.etl.input.Inputer;
import com.seassoon.etl.outputer.impl.MongoOutputer;
import com.seassoon.utils.ConnectionDB;
import com.seassoon.utils.MongoDBUtils;

public class MongoDbInputer extends AbstractInputer implements Inputer {

	/**
	 * 数据库
	 */
	private String dbName;

	private String collectionName;

	/**
	 * ip
	 */
	private String IP = null;

	/**
	 * 端口
	 */
	private Integer PORT = null;

	private static Logger log = Logger.getLogger(MongoDbInputer.class);

	private MongoDBUtils mongoDBUtils;

	private List<String> columnsList;

	public MongoDbInputer(String dbName, String collectionName, String IP, Integer PORT, List<String> columnsList) {

		this.dbName = dbName;
		this.collectionName = collectionName;

		this.IP = IP;
		this.PORT = PORT;

		this.columnsList = columnsList;

		init();

	}

	public void init() {
		if (queue == null) {
			queue = new LinkedBlockingQueue<>(100000);
		}
		if (mongoDBUtils == null) {
			mongoDBUtils = MongoDBUtils.getInstance(IP, PORT);
		}

		initThread = new Thread(new Runnable() {

			@Override
			public void run() {

				int currentCount = 0;

				MongoCursor<Document> mongoCursor = mongoDBUtils.getCollection(dbName, collectionName).find().projection(Filters.eq("_id", 0))
						.iterator();

				while (mongoCursor.hasNext()) {
					// System.out.println(mongoCursor.next());

					Document document = mongoCursor.next();

					currentCount++;

					String[] tempData = null;
					try {
						if (currentCount == 1) {
							//把字段存储起来

							originalColumnsList = new ArrayList<>();
							
							
							for (String string : document.keySet()) {
								originalColumnsList.add(string);
							}

							columnConvert();

						}

						if (columnsList == null || columnsList.size() == 0) {

							// columnsList = originalColumnsList;
							for (int i = 0; i < originalColumnsList.size(); i++) {
								columnsList.add(originalColumnsList.get(i));
							}
						}

						tempData = new String[columnsList.size()];

						for (int i = 0; i < originalColumnsList.size(); i++) {

							// if (columnsList == null ||
							// columnsList.size() == 0) {
							int columnIndex = columnsList.indexOf(originalColumnsList.get(i));
							if (columnIndex >= 0) {

								
								  if(document.get(originalColumnsList.get(i)) != null){
									  tempData[columnIndex] = document.get(originalColumnsList.get(i)).toString();
								  }
								  
								 
							}
							// } else {
							// tempData[i] = data[i];
							// }

						}

						queue.put(tempData);
						inputNum.incrementAndGet();

					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

			}
		});
		initThread.start();
	}

}
