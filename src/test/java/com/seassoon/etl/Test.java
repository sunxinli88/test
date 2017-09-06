package com.seassoon.etl;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.seassoon.common.config.JfinalORMConfig;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JfinalORMConfig config = new JfinalORMConfig();

		List<Record> list = Db.find("show full fields from judgement_ZSCQ");

		String[] keys = { "著作权", "商标", "专利", "不正当竞争" };
		
//		String[] keys = { "著作权" };

		String sql = null;
				
		List<String> sqlList  =new ArrayList<>();

		int i =0;
		
		for (String key : keys) {

			for (Record record : list) {
				
				String COMMENT = record.getStr("Comment").equals("")?record.getStr("Field"):record.getStr("Comment");
				
				i++;
//				if(i==3){
//					break;
//				}
				
				sql = "select  '{$COMMENT}',count({$Field})     from  judgement_ZSCQ where judgement_category='民事判决书'  and case_type_after like '%{$key}%' and  {$Field} is not null  and  {$Field} !='' and  {$Field}!='[]' ";

				sql = sql.replace("{$Field}", record.getStr("Field"));
				sql = sql.replace("{$key}", key);
				sql = sql.replace("{$COMMENT}", COMMENT);
				
//				System.out.println(sql);
				sqlList.add(sql);
			}
			System.out.println(Joiner.on(" union ").join(sqlList));
			sqlList.clear();
		}
		
	
		
		
//		List<Record> result = Db.find(Joiner.on(" union ").join(sqlList) );
//		
//		for (Record record : result) {
//			System.out.println(record);
//		}

	}

}
