package com.seassoon.hbzyy.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;

import com.google.common.base.Joiner;
import com.mongodb.client.model.Filters;
import com.seassoon.etl.input.impl.CsvInputer;
import com.seassoon.etl.input.impl.JDBCInputer;
import com.seassoon.etl.input.impl.MysqlInputer;
import com.seassoon.etl.outputer.impl.CsvOutputer;
import com.seassoon.etl.outputer.impl.JDBCOutputer;
import com.seassoon.etl.outputer.impl.MysqlOutputer;
import com.seassoon.utils.JDBC_DRIVER;
import com.seassoon.utils.MongoDBUtils;
import com.seassoon.utils.RegxUtils;

import au.com.bytecode.opencsv.CSVReader;

public class 临时导入2 {

	public static void 病人基本信息() {

		List<String> columnsList = new ArrayList<>();

		Map<String, String> columnConvertMap = new HashMap();

		columnConvertMap.put("OURID", "patientId");
		columnConvertMap.put("PATINAME", "name");
		columnConvertMap.put("SEX", "gender");
		columnConvertMap.put("BIRTHDAY", "birthday");

		CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\1-病人基本信息（门诊+住院）\\病人基本信息.csv.txt", columnsList,
				columnConvertMap, "GBK");
		csvInputer.setQueueSize(20000);

		JDBCOutputer outputer = new JDBCOutputer("view_patient_info", columnsList,
				"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8", "root",
				"SXAD13579@$^*)", JDBC_DRIVER.MYSQL.getValue());
		csvInputer.setQueueSize(20000);

		csvInputer.out(outputer);

	}

	public static void 检验() {

		List<String> columnsList = new ArrayList<>();

		Map<String, String> columnConvertMap = new HashMap();

		columnConvertMap.put("OURID", "patientId");
		columnConvertMap.put("PATINAME", "name");
		columnConvertMap.put("SEX", "gender");
		columnConvertMap.put("BIRTHDAY", "birthday");

		CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\1-病人基本信息（门诊+住院）\\病人基本信息.csv.txt", columnsList,
				columnConvertMap, "GBK");
		csvInputer.setQueueSize(20000);

		JDBCOutputer outputer = new JDBCOutputer("view_patient_info", columnsList,
				"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8", "root",
				"SXAD13579@$^*)", JDBC_DRIVER.MYSQL.getValue());
		csvInputer.setQueueSize(20000);

		csvInputer.out(outputer);

	}
	
	
	public static  void 完整病例数据提取(){
		


		List<String> columnsList = new ArrayList<>();


		CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\8-病历\\完整病历\\完整病历\\完整病历2.csv",
				columnsList, null, "GBK");
		csvInputer.setQueueSize(20000);

		CsvOutputer outPuter = null;
		try {
			outPuter = new CsvOutputer("I:\\工作目录\\中医院\\8-病历\\完整病历\\完整病历\\住院诊断.csv", columnsList);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		
		
		
		String[] data = null;
		while ((data = csvInputer.next()) != null) {
			try {
				
				if(!data[8].equals("结构化")){
					continue;
				}
				
//				if(!data[5].equals("黄疸")){
//					continue;
//				}
//				
				if(!data[10].contains("诊断")){
					continue;
				}
				
				if(!data[11].contains("入院病历")){
					continue;
				}
				
					System.out.println( "\""+Joiner.on("\",\"").useForNull("").join(data)+"\"" );
			
			
				
					outPuter.process(data);
				//return;
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
		
		outPuter.close();
		
	}
	
	public static  void 高速测试(){
		


		List<String> columnsList = new ArrayList<>();


		CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\江西高速测试数据\\江西高速测试数据\\收费流水2013.txt",
				columnsList, null, "UTF-8","	",false);
		csvInputer.setQueueSize(20000);

		
		
		
		
		CsvOutputer outPuter = null;
		try {
			outPuter = new CsvOutputer("I:\\工作目录\\江西高速测试数据\\江西高速测试数据\\收费流水2013_拆列.csv", columnsList);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		
		
		int zearo_count=1;
		int i=0;
		String[] data = null;
		List<String> tempData = null;
		while ((data = csvInputer.next()) != null) {
			try {
				if(i==0){
					columnsList.add("C_19");
				}
			
				tempData= new ArrayList(Arrays.asList(data));
				i++;

				if(data[11].equals("0") || data.equals("空")){
				
					
					tempData.add(null);
				}else{
					tempData.add(RegxUtils.getMatcherGroup("[\\u4e00-\\u9fa5]{2}", data[11]));
				}
				
				
				if(data[11].contains("蓝赣")){
					zearo_count++;
				}
				
				
				
				//System.out.println(zearo_count);
					//System.out.println( "\""+Joiner.on("\",\"").useForNull("").join(data)+"\"" );
			
			
				
//					outPuter.process(tempData.toArray(new String[tempData.size()]));
					
					
					
//					if(outPuter.processNum.intValue()>10000){
//						break;
//					}
				//return;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		outPuter.close();
		
	}

	public static void main(String[] args) throws IOException {


		BufferedReader bufferedReader =new BufferedReader(new InputStreamReader(new FileInputStream(new File("I:\\工作目录\\江西高速测试数据\\江西高速测试数据\\收费流水2013.txt")), "UTF-8"));
		
		String line =null;
		//Pattern pattern = Pattern.compile(",\\(");    
       int i =0;
		while( (line =bufferedReader.readLine()) != null){
			i++;
			
			if(line.contains("-1970")){
				System.out.println(i+" "+ line);
			}else{
				//System.out.println(line);
			}	
			
		
		
		}
		
	}

}
