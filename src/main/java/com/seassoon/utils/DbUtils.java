package com.seassoon.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.DbKit;

public class DbUtils { 
 
	private static Logger log = Logger.getLogger(DbUtils.class);
	
	public static void insert(List<String> columnsList, List<Object> paramsList, String tableName) {
		log.info(" insert start");
		DbUtils.insert(columnsList.toArray(new String[columnsList.size()]), paramsList.toArray(), tableName);

		paramsList.clear();
		log.info(" insert end");
	}
	
	
	public static void insert(String[] colums,Object[] params, String tableName){
		
		if(params==null || params.length==0){
			System.out.println(tableName+" 新增:0");
			return;
		}
		
		StringBuffer  sql =new StringBuffer();
		
		sql.append(" insert ignore into `"+tableName+"`  ( `"+Joiner.on("`,`").join(colums)+"`  )   values  ");
		
		
		
		List<String> recordList= new ArrayList<>();
		List<String>  paramList=new ArrayList<>();
		
		for (int i = 1; i <= params.length; i++) {
			
			paramList.add("?");
			
			if(i>=colums.length && i%colums.length==0){
				recordList.add( "("+Joiner.on(",").join(paramList) +")");
				paramList.clear();
			}
			
			
		}
		
		sql.append( Joiner.on(",").join(recordList));
		
		
//		System.out.println(sql .toString());
		
		
		log.info(tableName+"   submit ");
		int result= Db.update(sql.toString(), params);
	
		
		System.out.println(tableName+" 新增:"+result);

		
		
		
	}
	
	
	
	

}
