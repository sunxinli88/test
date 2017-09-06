package com.seassoon.jinhua.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jfinal.plugin.activerecord.Db;
import com.mysql.jdbc.Buffer;
import com.seassoon.common.config.JfinalORMConfig;
import com.seassoon.etl.input.impl.CsvInputer;
import com.seassoon.etl.input.impl.MysqlInputer;
import com.seassoon.etl.outputer.impl.MysqlOutputer;

import net.sf.json.JSONObject;

public class JudgementCompanyImportDataService {

	public List<String> provinceList;

	public JudgementCompanyImportDataService() {

		if (provinceList == null) {

			provinceList = Db.query("select name from  provinces");
		}
	}

	public void ImportData() {

		

		//
		// MysqlInputer inputer =new MysqlInputer("select * from common_drug
		// where id >7800078", columnsList, null);
		//
		// MysqlOutputer outputer = new MysqlOutputer("common_drug_copy",
		// columnsList);
		// //outputer.setSubmitSize(30000);
		//
		// inputer.out(outputer);
		//

		File file = new File("H:\\工作文件夹\\ex2.json");

		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String line = null;

		Set<String> plaintiffSet = new HashSet<>();
		Set<String> defendantSet = new HashSet<>();

		try {
			while ((line = bufferedReader.readLine()) != null) {

				JSONObject jsonObject = JSONObject.fromObject(line);

				String key = "原告";
				plaintiffSet.addAll(this.getData(key, jsonObject.get(key).toString()));
				key="被告";
				defendantSet.addAll(this.getData(key, jsonObject.get(key).toString()));
			

			}
			
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		final List<String> columnsList = Arrays.asList(new String[] { "category", "name","province" });
		
		Db.update("TRUNCATE judgement_plaintiff");
		Db.update("TRUNCATE judgement_defendant");


		
		
		
		MysqlOutputer outputer = new MysqlOutputer("judgement_plaintiff", columnsList);
		
		String[] data;
		for (String content : plaintiffSet) {
			
			data=new String[columnsList.size()];
			
			data[0]= this.getType(content);
			data[1]= content;
			data[2]= this.getProvince(content);
			
			
			outputer.process(data);
		}
		outputer.process(null);
		
		outputer = new MysqlOutputer("judgement_defendant", columnsList);
		

		for (String content : defendantSet) {
			
			data=new String[columnsList.size()];
			
			data[0]= this.getType(content);
			data[1]= content;
			data[2]= this.getProvince(content);
			
			
			outputer.process(data);
		}
		outputer.process(null);	

	}

	public Set<String> getData(String key, String content) {

		Set<String> set = new HashSet<>();
		String[] tempArray = content.split("、");
		for (String string : tempArray) {

			int index = string.indexOf("等");
			if (index > 0) {
				string = string.substring(0, index - 1);
			}
			index = string.lastIndexOf("公司");
			if (index != -1 && index + 2 < string.length()) {
				string = string.substring(0, index + 2);
			}
			string = string.replace(key, "");
			string=string.replaceAll("\\(\\d+\\)", "");
			string=string.replaceAll("\\d", "");
			
			
//			System.out.println(key + "：" + string + " 省：" + getProvince(string) + " 身份：" + getType(string));

			if(string.equals("")){
				continue;
			}
			set.add(string);
		}

		return set;
	}

	public String getProvince(String content) {

		for (String string : provinceList) {
			if (content.contains(string)) {
				return string;
			}
		}
		return null;

	}

	public String getType(String content) {

		if (content.length() <= 3) {
			return "个人";
		} else if (content.contains("公司")) {
			return "公司";
		} else {
			return "组织";
		}

	}

	public static void main(String[] args) {
		JfinalORMConfig config = new JfinalORMConfig();

		JudgementCompanyImportDataService dataService = new JudgementCompanyImportDataService();

		// File dir = new File("H:\\工作文件夹\\金华联通\\jinhuadata1");
		//
		// for (File file : dir.listFiles()) {
		//
		// if (file.getName().contains("201604") ||
		// file.getName().contains("201605")
		// || file.getName().contains("201606")) {
		//
		// dataService.ImportData(file.getPath());
		//
		// }
		// }

		dataService.ImportData();
	}

}
