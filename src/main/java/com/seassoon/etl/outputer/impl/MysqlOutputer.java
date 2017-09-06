package com.seassoon.etl.outputer.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.seassoon.etl.outputer.AbstractOutPuter;
import com.seassoon.etl.outputer.OutPuter;
import com.seassoon.utils.DbUtils;

public class MysqlOutputer extends   AbstractOutPuter implements OutPuter {

	public String sql;


	private static Logger log = Logger.getLogger(MysqlOutputer.class);




	private String tableName;

	private Integer submitSize = 100000;

	private List<String[]> list = new ArrayList<>();

	public Integer getSubmitSize() {
		return submitSize;
	}

	public void setSubmitSize(Integer submitSize) {
		this.submitSize = submitSize;
	}

	public MysqlOutputer( String tableName, List<String> columnsList) {

		this.columnsList = columnsList;

		this.tableName = tableName;

		init();

	}

	private void init() {
		// TODO Auto-generated method stub

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
		DbUtils.insert(columnsList, paramsList, tableName);
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
