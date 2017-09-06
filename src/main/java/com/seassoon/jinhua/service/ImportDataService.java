package com.seassoon.jinhua.service;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.seassoon.common.config.JfinalORMConfig;
import com.seassoon.etl.input.impl.CsvInputer;
import com.seassoon.etl.outputer.impl.MysqlOutputer;

public class ImportDataService {
	
	public  void  ImportData(String filePath){
		Map<String, String> columnConvertMap = new HashMap();

		columnConvertMap.put("DECODE(USR_STS_CD,'1','开机','", "STATUS");
		System.out.println(filePath);
		
		final List<String> columnsList = new ArrayList<>();
		
		
		CsvInputer csvInputer = new CsvInputer(filePath, columnsList, columnConvertMap);
		csvInputer.setQueueSize(20000);
		
		
		
		MysqlOutputer outputer = new MysqlOutputer("data", columnsList);
		outputer.setSubmitSize(30000);
		
		csvInputer.out(outputer);
		
		
		
	}

	public static void main(String[] args) {
		JfinalORMConfig config = new JfinalORMConfig();

		ImportDataService dataService= new ImportDataService();

//		File dir = new File("H:\\工作文件夹\\金华联通\\jinhuadata1");
//
//		for (File file : dir.listFiles()) {
//
//			if (file.getName().contains("201604") || file.getName().contains("201605")
//					|| file.getName().contains("201606")) {
//
//				dataService.ImportData(file.getPath());
//				
//			}
//		}
		
		dataService.ImportData("H:\\工作文件夹\\金华联通\\jinhuadata1\\7月数据.csv");
	}

}
