package com.seassoon.jinhua.service;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.seassoon.common.config.JfinalORMConfig;
import com.seassoon.etl.input.impl.CsvInputer;
import com.seassoon.etl.input.impl.MysqlInputer;
import com.seassoon.etl.outputer.impl.MysqlOutputer;

public class TestImportDataService {
	
	public  void  ImportData(){
		
	
		final List<String> columnsList = new ArrayList<>();
		
		
		
		MysqlInputer inputer =new MysqlInputer("select * from common_drug where id >7800078", columnsList, null);
		
		MysqlOutputer outputer = new MysqlOutputer("common_drug_copy", columnsList);
		//outputer.setSubmitSize(30000);
		
		inputer.out(outputer);
		
		
		
	}

	public static void main(String[] args) {
		JfinalORMConfig config = new JfinalORMConfig();

		TestImportDataService dataService= new TestImportDataService();

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
		
		dataService.ImportData();
	}

}
