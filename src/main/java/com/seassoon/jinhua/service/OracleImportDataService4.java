package com.seassoon.jinhua.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.seassoon.common.config.JfinalORMConfig;
import com.seassoon.etl.input.impl.JDBCInputer;
import com.seassoon.etl.outputer.impl.CsvOutputer;
import com.seassoon.utils.JDBC_DRIVER;

public class OracleImportDataService4 {
	
	public  void  ImportData(){
		
	
		

		
		
	}

	public static void main(String[] args) {
		JfinalORMConfig config = new JfinalORMConfig();

		 

	 
		 

    	if(args==null || args.length==0){
    		System.out.println("start:");
    		args=new String[]{"select * from cl_dhhm_201607@jhqyzc where rownum<10"};
    		
    	
    	} 
    	
    	String sql = args[0];
    	String filePath = args.length<2?"E://temp.csv":args[1];
		
		OracleImportDataService4 dataService= new OracleImportDataService4();
		
		final List<String> columnsList = new ArrayList<>();
		 
		
		
		JDBCInputer inputer =new JDBCInputer("select * from test.abc", columnsList, null,"jdbc:oracle:thin:@192.168.222.27:1521:orcl","TEST","SXAD",JDBC_DRIVER.ORACLE.getValue());
		
//		JDBCInputer inputer =new JDBCInputer(sql, columnsList, null,"jdbc:oracle:thin:@130.36.57.40:1521:ORAJH","HZYS","123456",JDBC_DRIVER.ORACLE.getValue());
		
		 
		
		
		 
		
	
		try {
			CsvOutputer outPuter =new CsvOutputer(filePath, columnsList); 
			
			inputer.out(outPuter);

		} catch (IOException e) {
			// TODO Auto-generated catch block  
			e.printStackTrace();
		}
		
	}

}
