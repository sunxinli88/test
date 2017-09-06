package com.seassoon.etl;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.seassoon.etl.input.impl.CsvInputer;
import com.seassoon.etl.input.impl.MongoDbInputer;
import com.seassoon.etl.outputer.impl.MongoOutputer;
import com.seassoon.etl.outputer.impl.MysqlOutputer;
import com.seassoon.utils.MongoDBUtils;

public class MongoOuterTest {
	
	
	public static void main(String[] args) {
		
		final List<String> columnsList = new ArrayList<>();
		//表示列的字段，可以为空，说明没有给出字段的初始值
		
//		CsvInputer inputer = new CsvInputer("I:\\工作目录\\中医院\\5-医嘱\\ip_advice2012-2015.csv", columnsList, null);
//		inputer.setQueueSize(50000);
		
		MongoDbInputer inputer = new MongoDbInputer("runoob","col","localhost",27017,columnsList);
		inputer.setQueueSize(40000);
		
		MongoOutputer outputer = new MongoOutputer("runoob","a2","localhost",27017,columnsList);
		//outputer.setSubmitSize(10000);
		
//		csvInputer.out(outputer);
		
		
		String[] data = null;
		while (true) {
			data = inputer.next();

			if (data == null) {
		
				outputer.close();
				
				break;
			}

			outputer.process(data);
			
//			System.out.println(Joiner.on(",").useForNull("").join(data));
			//System.out.println(csvInputer.inputNum +" "+csvInputer.outNum);
		}
	}

}
