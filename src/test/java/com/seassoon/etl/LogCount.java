package com.seassoon.etl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

import com.seassoon.utils.ConnectionDB;

public class LogCount {

	public static String getMatcher(String regex, String source) {
		String result = "";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(source);
		while (matcher.find()) {
			result = matcher.group(1);
		}
		return result;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// JfinalORMConfig config = new JfinalORMConfig();

		ConnectionDB connectionDB =new ConnectionDB("jdbc:mysql://172.16.40.125:3306/mytest?useUnicode=true&characterEncoding=utf8", "root", "SXAD13579@$^*)");
		
		
		List<String> columnList=Arrays.asList(new String[]{"date","url","referer","user_agent","ip"});
		
		try {

			List<String> list = IOUtils
					.readLines(new FileInputStream("C:\\Users\\MY_DUYILIU\\Downloads\\log2.log"), "UTF-8");

			List<String[]> datas= new ArrayList<>();
			String[] data ;
			for (String string : list) {
				data = new String[5];
				String[] temp =string.split("\\|");
				data[0]=getMatcher(" (.*)\\+" , temp[0].split(" INFO: ")[0] ).replace("T", " ");
				data[1]=temp[0].split(" INFO: ")[1] ;
				data[2]=temp[1].replace("refer:", "") ;
				data[3]=temp[2] ;
				if(temp.length==4){
					data[4]=temp[3] ;
				}else{
					data[4]=null ;
				}
					System.out.println(data[2]); 
					datas.add(data);
			}
			
			
			List<Object> paramsList = new ArrayList<Object>();
			for (String[] temp : datas) {
				for (String string : temp) {
					paramsList.add(string);
				}
				
			}
			
			connectionDB.insert(connectionDB, columnList, paramsList, "log");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
