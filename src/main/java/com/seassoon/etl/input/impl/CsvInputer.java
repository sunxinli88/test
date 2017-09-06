package com.seassoon.etl.input.impl;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import com.google.common.base.Joiner;
import com.seassoon.etl.input.AbstractInputer;
import com.seassoon.etl.input.Inputer;

import au.com.bytecode.opencsv.CSVReader;

public class CsvInputer extends AbstractInputer implements Inputer {

	private String filePath;


	private Boolean isFirstHead = true;

	private String charsetName = "UTF-8";
	
	private String separator=",";
	
	
	
	private Boolean isHeadColumn =true;
	
	private  InputStream inputStream ;
	
	public CsvInputer(String filePath, List<String> columnsList, Map<String, String> columnConvertMap) {
		super();
		this.filePath = filePath;
		
		try {
			inputStream = new FileInputStream(filePath);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.columnsList = columnsList;

		this.columnConvertMap = columnConvertMap;
		init();
	}
	
	public CsvInputer(String filePath, List<String> columnsList, Map<String, String> columnConvertMap,
			String charsetName) {
		super();
		this.filePath = filePath;
		try {
			inputStream = new FileInputStream(filePath);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.columnsList = columnsList;

		this.columnConvertMap = columnConvertMap;

		this.charsetName = charsetName;

		init();
	}
	
	public CsvInputer(String filePath, List<String> columnsList, Map<String, String> columnConvertMap,
			String charsetName,String separator,Boolean isHeadColumn) {
		super();
		this.filePath = filePath;
		try {
			inputStream = new FileInputStream(filePath);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.columnsList = columnsList;

		this.columnConvertMap = columnConvertMap;

		this.charsetName = charsetName;
		this.separator = separator;
		
		this.isHeadColumn=isHeadColumn;
		init();
	}
	
	/**
	 * 
	 * @param fs
	 * @param columnsList
	 * @param columnConvertMap
	 * @param charsetName
	 */
	public CsvInputer(InputStream fs, List<String> columnsList, Map<String, String> columnConvertMap,
			String charsetName,String separator,Boolean isHeadColumn) {
		super();
		//this.filePath = filePath;
		inputStream = fs;

		this.columnsList = columnsList;

		this.columnConvertMap = columnConvertMap;

		this.charsetName = charsetName;
		this.separator = separator;
		
		this.isHeadColumn=isHeadColumn;
		init();
	}
	
	/**
	 * 
	 * @param fs
	 * @param columnsList
	 * @param columnConvertMap
	 * @param charsetName
	 */
	public CsvInputer(InputStream fs, List<String> columnsList, Map<String, String> columnConvertMap,
			String charsetName) {
		super();
		//this.filePath = filePath;
		inputStream = fs;

		this.columnsList = columnsList;

		this.columnConvertMap = columnConvertMap;

		this.charsetName = charsetName;

		init();
	}

	private void init() {

		if (queue == null) {
			queue = new LinkedBlockingQueue<>(queueSize);
		}

		initThread = new Thread(new Runnable() {

			@Override
			public void run() {
				int currentCount = 0;
				String[] tempData = null;

				ICsvListReader listReader = null;
				String[] header =null;
				try {
					
					if(separator.equals(",")){
						listReader = new CsvListReader(new InputStreamReader(inputStream, charsetName),
								CsvPreference.STANDARD_PREFERENCE);
					}else if(separator.equals("	")){
						listReader = new CsvListReader(new InputStreamReader(inputStream, charsetName),
								CsvPreference.TAB_PREFERENCE);
					}
				
					List<String> customerList;
					//String[] data = null;
					while ((customerList = listReader.read()) != null) {

						// for (String[] data : list) {

						currentCount++;

						try {							
						
							if ( currentCount == 1) {
								//------------************-----------功能是什么
								for (int i = 0; i < customerList.size(); i++) {
							
									customerList.set(i, customerList.get(i).replace("", ""));									
								}
								
								if(isHeadColumn != true ){
									//判断如果没有给出表格的头部，生成一系列 C_i 的列名称
									//存储列名称
									originalColumnsList = new ArrayList<>();
									
									for (int i = 0; i < customerList.size(); i++) {

										originalColumnsList.add(String.format("C_" + (i + 1)));										
									}
															
								}else{
									//第一次读取的就是表头，赋值给 originalColumnsList
									originalColumnsList = customerList;
								}						
								columnConvert();
							}
							
							
							if (currentCount > 1 || (currentCount == 1 && isHeadColumn==false )) {
//							else {

								if (columnsList == null || columnsList.size() == 0) {

									// columnsList = originalColumnsList;
									for (int i = 0; i < customerList.size(); i++) {
										columnsList.add(originalColumnsList.get(i));
									}
								}

								tempData = new String[columnsList.size()];

								for (int i = 0; i < originalColumnsList.size(); i++) {

									// if (columnsList == null ||
									// columnsList.size() == 0) {
									int columnIndex = columnsList.indexOf(originalColumnsList.get(i));
									if (columnIndex >= 0) {
										tempData[columnIndex] = customerList.get(i);
									}
									// } else {
									// tempData[i] = data[i];
									// }

								}						
								queue.put(tempData);
								inputNum.incrementAndGet();
							}

						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
					inputStream.close();
					listReader.close();
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		});
		initThread.start();

	}

	// @Override
	// public String[] next() {
	// while (initThread.isAlive()) {
	// String[] data = queue.poll();
	// if (data != null) {
	// return data;
	// }
	//
	// try {
	// Thread.sleep(5000);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// return null;
	// }

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}



	public static void main(String[] args) {
		final List<String> columnsList = new ArrayList<>();
		CsvInputer csvInputer = new CsvInputer("C:\\Users\\xx\\Desktop\\test.csv", columnsList, null,"GBK");

		String[] data = null;
		while ((data = csvInputer.next()) != null) {
			System.out.println(Joiner.on(",").useForNull("").join(data));
		}

	}

}
