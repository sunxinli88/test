package com.seassoon.etl.outputer.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.graphbuilder.curve.Point;
import com.seassoon.etl.outputer.AbstractOutPuter;
import com.seassoon.etl.outputer.OutPuter;
import com.seassoon.utils.MongoDBUtils;

public class MongoOutputer extends   AbstractOutPuter implements OutPuter {
 


	/**
	 * 数据库
	 */
	private String dbName ;
	
	
	private String collectionName;
	
	/**
	 * ip
	 */
	private String IP = null;

	/**
	 * 端口
	 */
	private Integer PORT = null;


	
	private static Logger log = Logger.getLogger(MongoOutputer.class);

	private MongoDBUtils mongoDBUtils;


	
	


	private Integer submitSize = 20000;

	private List<String[]> list = new ArrayList<>();

	public Integer getSubmitSize() {
		return submitSize;
	}

	public void setSubmitSize(Integer submitSize) {
		this.submitSize = submitSize;
	}

	public MongoOutputer(String dbName,String collectionName,String IP,Integer PORT  , List<String> columnsList) {

		this.dbName=dbName;
		this.collectionName=collectionName;
		
		this.IP=IP;
		this.PORT=PORT;
		
		this.columnsList = columnsList;


		init();

	}
	
	
	private void init() {
		if (mongoDBUtils == null) {
			mongoDBUtils =MongoDBUtils.getInstance(IP, PORT);
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
	
		Document doc;
//		for (String[] temp : list) {
//			
//			for (String string : temp) {
//				paramsList.add(string);
//			}
//			
//		}
	
		List<Document> docs =new ArrayList<>();
		for (String[] temp : list) {
			doc=new Document();
			for (int i = 0; i < temp.length; i++) {
				doc.append(columnsList.get(i).replace(".", ""), temp[i]);
			}
			docs.add(doc);
			
		}
		
 		mongoDBUtils.insertMany(dbName, collectionName, docs);
		//db.insert(columnsList.toArray(new String[columnsList.size()]), paramsList.toArray(), tableName);
		list.clear();
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
