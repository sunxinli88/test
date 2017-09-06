package com.seassoon.etl.outputer.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.jfinal.plugin.activerecord.Record;
import com.seassoon.etl.outputer.AbstractOutPuter;
import com.seassoon.etl.outputer.OutPuter;
import com.seassoon.utils.DbUtils;

import au.com.bytecode.opencsv.CSVWriter;

public class CsvOutputer extends AbstractOutPuter implements OutPuter {

	public BlockingQueue<String[]> queue;

	private static Logger log = Logger.getLogger(CsvOutputer.class);

	private List<String> columnsList;

	private String filePath;

	private Integer submitSize = 10000;

	private List<String[]> list = new ArrayList<>();

	Writer writer = null;

	CSVWriter csvWriter;
	
	boolean is_print_head=false;

	public CsvOutputer(String filePath, List<String> columnsList) throws IOException {

		this.columnsList = columnsList;

		this.filePath = filePath;

		init();

	}

	private void init() throws IOException {

		writer = new FileWriter(filePath);

		csvWriter = new CSVWriter(writer, ',');
		
	
	
		
		
	}

	@Override
	public void process(String[] data) {
//		if(is_print_head ==false){
//			csvWriter.writeNext(columnsList.toArray(new String[columnsList.size()]));
//			is_print_head=true;
//		}
//	

		processNum.incrementAndGet();

		list.add(data);
		if (list.size() > submitSize) {
			execute();
		}

		

		
	}

	public void execute() {
		

		for (String[] temp : list) {

			csvWriter.writeNext(temp);

		}
		
		
		list.clear();
		
		log.info("complete:"+processNum);

	}

	public void close() {
		this.execute();
		try {
			this.csvWriter.close();
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
