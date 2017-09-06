package com.seassoon.etl.outputer.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.jfinal.plugin.activerecord.Record;
import com.mchange.v2.async.StrandedTaskReporting;
import com.seassoon.etl.outputer.AbstractOutPuter;
import com.seassoon.etl.outputer.OutPuter;
import com.seassoon.utils.DbUtils;

import au.com.bytecode.opencsv.CSVWriter;

public class CsvStreamOutputer extends AbstractOutPuter implements OutPuter {

	public BlockingQueue<String[]> queue;

	private static Logger log = Logger.getLogger(CsvStreamOutputer.class);

	private List<String> columnsList;

	private String filePath;

	private Integer submitSize = 10000;

	private List<String[]> list = new ArrayList<>();

	Writer writer = null;

	CSVWriter csvWriter;
	
	BufferedWriter bufferedWriter;
	
	StringWriter stringWriter;
	
	private  OutputStream outputStream ;
	
	boolean is_print_head=false;

	public CsvStreamOutputer(String filePath, List<String> columnsList) throws IOException {

		this.columnsList = columnsList;

		this.filePath = filePath;
		
		this.outputStream= new FileOutputStream(new File(filePath));

		init();

	}
	
	public CsvStreamOutputer(OutputStream outputStream, List<String> columnsList) throws IOException {

		this.columnsList = columnsList;

		this.filePath = filePath;
		
		this.outputStream= outputStream;

		init();

	}

	private void init() throws IOException {

		//writer = new FileWriter(filePath);
		stringWriter=new StringWriter();
		csvWriter = new CSVWriter(stringWriter);
		
		bufferedWriter=new BufferedWriter(new OutputStreamWriter(outputStream));
	
		
		
	}

	@Override
	public void process(String[] data) {

	

		processNum.incrementAndGet();

		list.add(data);
		if (list.size() > submitSize) {
			execute();
		}

		

		
	}

	

	public void execute() {
		
		
		
		if(is_print_head ==false){
			//csvWriter.writeNext(columnsList.toArray(new String[columnsList.size()]));
			
			//csvWriter.writeNext(columnsList.toArray(new String[columnsList.size()]));
			
			try {
				bufferedWriter.write(convertData(columnsList.toArray(new String[columnsList.size()])));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//bufferedWriter.write("\\n");
			
			is_print_head=true;
		}
		
		int i =0;
		for (String[] temp : list) {
			i++;
			System.out.println(i);
			//csvWriter.writeNext(temp);
			//csvWriter.writeNext(temp);

			try {
				bufferedWriter.write(convertData(temp));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		list.clear();
		
		log.info("complete:"+processNum);

	}
	
	String tempResult=null;
	public String convertData(String[] nextLine){
//		csvWriter.writeNext(nextLine);
//		
//		tempResult= stringWriter.toString();
//		stringWriter.flush();
		
		
		return "\""+Joiner.on("\",\"").useForNull("").join(nextLine)+"\"\n";
	}

	public void close() {
		this.execute();
		try {
			this.csvWriter.close();
			this.bufferedWriter.flush();
			this.bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
