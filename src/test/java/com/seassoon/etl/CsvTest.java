package com.seassoon.etl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.common.base.Joiner;
import com.seassoon.etl.input.impl.CsvInputer;
import com.seassoon.etl.outputer.impl.CsvOutputer;
import com.seassoon.etl.outputer.impl.CsvStreamOutputer;
import com.seassoon.utils.ConnectionDB;

public class CsvTest {
/**
 * CSV文件-->数据库
 * 
 * 
 * */
	public static void main(String[] args) throws SQLException, ParseException {
		final List<String> columnsList = new ArrayList<>();
		
		CsvInputer inputer =new CsvInputer("C:\\Users\\xx\\Desktop\\test.csv", columnsList, null,"GBK");
		
		try {
			//CsvStreamOutputer outPuter = new CsvStreamOutputer("H:\\工作文件夹\\icd2.csv", columnsList);

			
			CsvStreamOutputer outPuter = new CsvStreamOutputer(new FileOutputStream(new File("C:\\Users\\xx\\Desktop\\test23.csv")), columnsList);
			
			//inputer.out(outPuter);
			ConnectionDB db = new ConnectionDB("jdbc:mysql://localhost:3306/aa?useUnicode=true&characterEncoding=utf8","root","123");
			
			
			String[] data = null;
			
			List<String> list =new ArrayList<>();
			
			while (true) {
				data = inputer.next();//每次取出 String 类型的数组

				if (data == null) {
			
					outPuter.close();
					
					break;
				}

				//outPuter.process(data);
				
				for(int j=0;j<data.length;j++){
					//val.append(data[j]+'-');
					if(j%10 == 3){
						SimpleDateFormat dateFormat =new SimpleDateFormat("dd/MM/yyyy");
						
						Date date =	dateFormat.parse(data[j]);
						
						SimpleDateFormat dateFormat2 =new SimpleDateFormat("yyyy-MM-dd");
						
						data[j] = dateFormat2.format(date);
					}
					list.add(data[j]);
				}
				System.out.println(Joiner.on(",").useForNull("").join(data));
				
			}

			String[] columns = { "customerNo", "firstName","lastName","birthDate","mailingAddress"
					,"married","numberOfKids","favouriteQuote","email","loyaltyPoints" };
			
			String[] values=list.toArray(new String[list.size()]);
			
			db.insert(columns, values, "cc");
			
			ResultSet resultSet = db.executeQueryRS("select * from cc", null);
			while (resultSet.next()) {
				//		Map<String, Object> map = new HashMap<String, Object>();
				for(int i=1;i<=columns.length;i++){
					System.out.print(resultSet.getObject(i)+",");
				}	
				System.out.println();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
