package com.seassoon.hbzyy.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.seassoon.etl.input.impl.CsvInputer;
import com.seassoon.etl.input.impl.JDBCInputer;
import com.seassoon.etl.input.impl.MysqlInputer;
import com.seassoon.etl.outputer.impl.JDBCOutputer;
import com.seassoon.etl.outputer.impl.MysqlOutputer;
import com.seassoon.utils.ConnectionDB;
import com.seassoon.utils.JDBC_DRIVER;

public class 完整病历2导入 { 
	
	
	static ConnectionDB  db =new ConnectionDB("jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
	
	
	public static void  病人基本信息(){
		
		
		 List<String> columnsList = new ArrayList<>();
		 
		 
		 Map<String, String> columnConvertMap = new HashMap();

		columnConvertMap.put("OURID", "patientId");
		columnConvertMap.put("PATINAME", "name");
		columnConvertMap.put("SEX", "gender");
		columnConvertMap.put("BIRTHDAY", "birthday");	
	
			
			
			CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\1-病人基本信息（门诊+住院）\\病人基本信息.csv.txt", columnsList, columnConvertMap,"GBK");
			csvInputer.setQueueSize(20000);
			
			
			JDBCOutputer outputer =new JDBCOutputer("view_patient_info",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			csvInputer.setQueueSize(20000);
			
			
			csvInputer.out(outputer);
		
	
		
		
	}
	
	
	public static void  op_register(){
		
		
		 List<String> columnsList = new ArrayList<>();
		 
		 
		 Map<String, String> columnConvertMap = new HashMap();
//
//		columnConvertMap.put("OURID", "patientId");
//		columnConvertMap.put("PATINAME", "name");
//		columnConvertMap.put("SEX", "gender");
//		columnConvertMap.put("BIRTHDAY", "birthday");	
	
			
			
			CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\6-门诊处方\\op_register2016.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			JDBCOutputer outputer =new JDBCOutputer("op_register",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			csvInputer.setQueueSize(10000);
			
			
			csvInputer.out(outputer);
			
			
		 	csvInputer = new CsvInputer("I:\\工作目录\\中医院\\6-门诊处方\\op_register.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			outputer =new JDBCOutputer("op_register",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			csvInputer.setQueueSize(10000);
		
			csvInputer.out(outputer); 
		
		 
	}
	
	public static void  op_diagnose(){
		
		
		 List<String> columnsList = new ArrayList<>();
		 
		 
		 Map<String, String> columnConvertMap = new HashMap();

			
			CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\6-门诊处方\\op_diagnose2016.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			JDBCOutputer outputer =new JDBCOutputer("op_diagnose",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			csvInputer.setQueueSize(20000);
			
			
			csvInputer.out(outputer);
			
			
		 	csvInputer = new CsvInputer("I:\\工作目录\\中医院\\6-门诊处方\\op_diagnose.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			outputer =new JDBCOutputer("op_diagnose",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			csvInputer.setQueueSize(20000);
		
			csvInputer.out(outputer); 
//		
		 
	}
	
	
	
	public static void  op_recipe(){
		
		
		 List<String> columnsList = new ArrayList<>();
		 
		 
		 Map<String, String> columnConvertMap = new HashMap();

			
			CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\6-门诊处方\\op_recipe2016.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			JDBCOutputer outputer =new JDBCOutputer("op_recipe",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			csvInputer.setQueueSize(20000);
			
			
			csvInputer.out(outputer);
			
			
		 	csvInputer = new CsvInputer("I:\\工作目录\\中医院\\6-门诊处方\\op_recipe_处方主表.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			outputer =new JDBCOutputer("op_recipe",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			csvInputer.setQueueSize(20000);
		
			csvInputer.out(outputer); 
//		
		 
	}
	
	
	public static void  op_da(){
		
		
		 List<String> columnsList = new ArrayList<>();
		 
		 
		 Map<String, String> columnConvertMap = new HashMap();

			
			CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\6-门诊处方\\op_da2016.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			JDBCOutputer outputer =new JDBCOutputer("op_da",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			csvInputer.setQueueSize(20000);
			
			
			csvInputer.out(outputer);
			
			
		 	csvInputer = new CsvInputer("I:\\工作目录\\中医院\\6-门诊处方\\op_da_处方明细.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			outputer =new JDBCOutputer("op_da",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			csvInputer.setQueueSize(20000);
		
			csvInputer.out(outputer); 
//		
		 
	}

	
	public static void  op_costdtl(){
		
		
		 List<String> columnsList = new ArrayList<>();
		 
		 
		 Map<String, String> columnConvertMap = new HashMap();

			
			CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\6-门诊处方\\op_costdtl2016.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			JDBCOutputer outputer =new JDBCOutputer("op_costdtl",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			outputer.setSubmitSize(20000);
			
			
			csvInputer.out(outputer);
			
			
		 	csvInputer = new CsvInputer("I:\\工作目录\\中医院\\6-门诊处方\\op_costdtl_手工处方.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			outputer =new JDBCOutputer("op_costdtl",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			outputer.setSubmitSize(20000);
		
			csvInputer.out(outputer); 
//		
		 
	}

	
	

	public static void  住院诊断(){
		
		
		 List<String> columnsList = new ArrayList<>();
		 
		 
		 Map<String, String> columnConvertMap = new HashMap();

			
			CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\8-病历\\完整病历\\完整病历\\住院诊断.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			JDBCOutputer outputer =new JDBCOutputer("住院诊断",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			outputer.setSubmitSize(20000);
			
			
			csvInputer.out(outputer); 
			

//		
		 
	}
	
	
	public static void  ip_checkin(){
		
		
		 List<String> columnsList = new ArrayList<>();
		 
		 
		 Map<String, String> columnConvertMap = new HashMap();

			
			CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\2-入院信息\\ip_checkin.txt..csv", columnsList, columnConvertMap,"GBK");
			csvInputer.setQueueSize(20000);
			
			
			JDBCOutputer outputer =new JDBCOutputer("ip_checkin",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			outputer.setSubmitSize(20000);
			
			
			csvInputer.out(outputer);
		
//		
		 
	}
	
	
	public static void  ip_advice(){
		
		
		 List<String> columnsList = new ArrayList<>();
		 
		 
		 Map<String, String> columnConvertMap = new HashMap();

			
			CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\5-医嘱\\ip_advice2015-2016.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			JDBCOutputer outputer =new JDBCOutputer("ip_advice",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			outputer.setSubmitSize(20000);
			
			
			csvInputer.out(outputer);
			
			
		 	csvInputer = new CsvInputer("I:\\工作目录\\中医院\\5-医嘱\\ip_advice2012-2015.csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(20000);
			
			
			outputer =new JDBCOutputer("ip_advice",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			outputer.setSubmitSize(20000);
		
			csvInputer.out(outputer); 
//		
		 
	}
	
	
	
	
	public static void  testReport_Items(){
		
		
		 List<String> columnsList = new ArrayList<>();
		 
		 
		 Map<String, String> columnConvertMap = new HashMap();

			
			CsvInputer csvInputer = new CsvInputer("I:\\工作目录\\中医院\\3-检验信息\\3-检验信息\\View_XKY_TestReport_Items(2).csv", columnsList, columnConvertMap,"UTF-8");
			csvInputer.setQueueSize(100000);
			
			
			JDBCOutputer outputer =new JDBCOutputer("testReport_Items",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
			outputer.setSubmitSize(100000);
			
			
			csvInputer.out(outputer);
		
//		
		 
	}
	
	
	
	
	
	
	
	
	public static void  import_to_hosp_diagnosis(){
		
		 List<String> columnsList = new ArrayList<>();
		
		 JDBCInputer inputer =	new JDBCInputer("SELECT ip_checkin.PATIID patientId, ip_checkin.PATINAME name, case when   ip_checkin.age is null then (year(now())-year(ip_checkin.BIRTHDAY)-1) + ( DATE_FORMAT(ip_checkin.BIRTHDAY, '%m%d') <= DATE_FORMAT(NOW(), '%m%d') ) else     ip_checkin.age  end  age  ,ip_checkin.SEX gender   , ip_checkin.IPNUMBER commonId,SBAS_EMPLOYEE.OURNAME doctorName,SBAS_DEPT.OURNAME departmentName , substring( 住院诊断.ITEMVALUE, LOCATE('.', 住院诊断.ITEMVALUE) + 1 ) diagnosis,  REPLACE ( 住院诊断.ELEMCOMP, '中西医', '' ) diagnosisType, REPLACE ( 住院诊断.ELEMNAME, '分层', '' ) diagnosisCategory, ip_checkin.INDATE admissionDate, ip_checkin.OUTDATE dischargeDate, ip_checkin.INDATE date FROM 住院诊断, ip_checkin,SBAS_EMPLOYEE,SBAS_DEPT WHERE 住院诊断.CASEID = ip_checkin.IPNUMBER AND 住院诊断.HNAME = ip_checkin.PATINAME AND SBAS_EMPLOYEE.OURID=ip_checkin.OPDOCTORID AND SBAS_DEPT.`﻿OURID`=ip_checkin.OPDEPTID and REPLACE ( 住院诊断.ELEMCOMP, '中西医', '' ) = '确定诊断' GROUP BY NAME, commonId, diagnosis, diagnosisType, diagnosisCategory", columnsList, null,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		
		
		 JDBCOutputer outputer =new JDBCOutputer("hosp_diagnosis",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 outputer.setSubmitSize(20000);
			
			
			inputer.out(outputer);
		
		
	}
	
	
	
	public static void  import_to_hosp_drug(){
		
		 List<String> columnsList = new ArrayList<>();
		 
		 db.executeUpdate("truncate hosp_drug");
		
		 JDBCInputer inputer =	new JDBCInputer("select ip_checkin.IPNUMBER   commonId , ip_advice.PATIID patientId,ip_advice.PATINAME name,ip_advice.RECIPEID prescriptionNo,ip_advice.STARTDOCTOR prescriptionDoctor, case when ip_advice.TREATTYPEID =11 then '西药' else '草药' end prescriptionType ,ip_advice.ADVICENAME drug,ip_advice.SPEC specifications,ip_advice.QUANTITY singleDose,ip_advice.CALCUNIT unit,ip_advice.FREQNAME drugUseFrequency,ip_advice.DOSE traditionalChineseMedicineNumber,ip_advice.QUANTITY*ip_advice.DOSE totalDrugDose,ip_advice.STARTEXECTIME physicianOrderStartDate   from ip_advice ,ip_checkin  where ip_checkin.OURID=ip_advice.IPID and   RECIPEID is not null and   TREATTYPEID in( 11,13)", columnsList, null,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		
		
		 JDBCOutputer outputer =new JDBCOutputer("hosp_drug",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 outputer.setSubmitSize(20000);
			
			
			inputer.out(outputer);
		
		
	}
	
	
	public static void  import_to_hosp_test(){
		
		 List<String> columnsList = new ArrayList<>();
		 
		 db.executeUpdate("truncate hosp_test");
		
		 JDBCInputer inputer =	new JDBCInputer("select ip_checkin.PATIID patientId,t1.住院号 commonId,t1.患者姓名 name,t1.`检验报告单编号` reportNo,t1.标本类别 specimen, t1.`检验类别(检验小组)` testCategory,t1.`检验项目名称` testItem ,t1.`检验日期` testDate,t2.`检验项目名称` testProjectName,t2.`检验项目英文名称` testProjectNameEn,t2.`检验项目结果(定性)` testResultQualitative ,t2.`检验项目结果(定量)` testResultQuantitative,t2.`检验项目结果参考值` testResultReferenceValue,t2.`检验项目结果单位` testResultUnit,t2.`检验结果高低值` testResultHighOrLow from TestReport_Master     t1 left join ip_checkin on ip_checkin.IPNUMBER = t1.住院号 and  ip_checkin.PATINAME=t1.患者姓名   , `testReport_Items` t2  where t2.`检验报告单编号` =  t1.`检验报告单编号`   and  LENGTH(t1.住院号)<=7 and ip_checkin.OURID  is not null", columnsList, null,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 inputer.setQueueSize(100000);
		
		 JDBCOutputer outputer =new JDBCOutputer("hosp_test",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 outputer.setSubmitSize(100000);
			
			
		 inputer.out(outputer);
		
		
	}
	
	
	
	public static void  import_to_outp_diagnosis(){
		
		 List<String> columnsList = new ArrayList<>();
		 
		 db.executeUpdate("truncate outp_diagnosis");
		
		 JDBCInputer inputer =	new JDBCInputer("SELECT op_register.PATIID patientId, op_register.PATINAME NAME, op_register.age age, op_register.SEX gender, op_register.OURID commonId,SBAS_DEPT.OURNAME departmentName, SBAS_EMPLOYEE.OURNAME doctorName, op_diagnose.DISEASENAME diagnosis, op_register.CREATETIME date, op_diagnose.DISEASECODE icd_code FROM op_register left join SBAS_DEPT on SBAS_DEPT.`﻿OURID`=op_register.DEPTID, op_diagnose,(select * from SBAS_EMPLOYEE GROUP BY OURID ) SBAS_EMPLOYEE WHERE op_register.OURID = op_diagnose.REGISTERID AND op_diagnose.CREATORID = SBAS_EMPLOYEE.OURID", columnsList, null,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 inputer.setQueueSize(100000);
		
		 JDBCOutputer outputer =new JDBCOutputer("outp_diagnosis",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 outputer.setSubmitSize(100000);
			
			
		 inputer.out(outputer);
		
		
	}
	
	
	
	public static void  import_to_outp_test(){
		
		 List<String> columnsList = new ArrayList<>();
		 
		 db.executeUpdate("truncate outp_test");
		
		 JDBCInputer inputer =	new JDBCInputer("select op_register.PATIID patientId,t1.住院号 commonId,t1.患者姓名 name,t1.`检验报告单编号` reportNo,t1.标本类别 specimen, t1.`检验类别(检验小组)` testCategory,t1.`检验项目名称` testItem ,t1.`检验日期` testDate,t2.`检验项目名称` testProjectName,t2.`检验项目英文名称` testProjectNameEn,t2.`检验项目结果(定性)` testResultQualitative ,t2.`检验项目结果(定量)` testResultQuantitative,t2.`检验项目结果参考值` testResultReferenceValue,t2.`检验项目结果单位` testResultUnit,t2.`检验结果高低值` testResultHighOrLow from TestReport_Master     t1 left join (select * from op_register group by  CARECARD ) op_register on op_register.CARECARD = t1.住院号  , `testReport_Items` t2  where t2.`检验报告单编号` =  t1.`检验报告单编号`   and  LENGTH(t1.住院号)>7 and op_register.PATIID  is not null", columnsList, null,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 inputer.setQueueSize(100000);
		
		 JDBCOutputer outputer =new JDBCOutputer("outp_test",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 outputer.setSubmitSize(100000);
			
			
		 inputer.out(outputer);
		
		
	}
	
	
	
	
	public static void  import_to_outp_drug(){
		
		 List<String> columnsList = new ArrayList<>();
		 
		 db.executeUpdate("truncate outp_drug");
		
		 JDBCInputer inputer =	new JDBCInputer("SELECT op_register.OURID commonId, op_register.PATIID patientId, op_register.PATINAME NAME, op_recipe.RECINUMBER prescriptionNo, SBAS_EMPLOYEE.OURNAME prescriptionDoctor, CASE WHEN op_da.EXECDEPTID = 258 THEN '西药' ELSE '草药' END prescriptionType, op_da.DANAME drug, op_da.QUANTITY singleDose, op_da.CALCUNIT unit, op_da.FREQNAME drugUseFrequency, op_da.DOSE traditionalChineseMedicineNumber, op_da.QUANTITY * op_da.DOSE totalDrugDose, op_da.CREATETIME prescriptionDate FROM op_recipe, op_register, ( SELECT * FROM SBAS_EMPLOYEE GROUP BY OURID ) SBAS_EMPLOYEE, op_da WHERE op_register.OURID = op_recipe.REGISTERID AND SBAS_EMPLOYEE.OURID = op_recipe.CREATORID AND op_da.RECIPEID = op_recipe.OURID AND op_da.EXECDEPTID IN (258, 259)", columnsList, null,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 inputer.setQueueSize(100000);
		
		 JDBCOutputer outputer =new JDBCOutputer("outp_drug",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 outputer.setSubmitSize(100000);
		 
		 inputer.out(outputer);
		 
		 inputer =	new JDBCInputer("SELECT op_register.OURID commonId, op_register.PATIID patientId, op_register.PATINAME NAME, op_recipe.RECINUMBER prescriptionNo, SBAS_EMPLOYEE.OURNAME prescriptionDoctor, CASE WHEN op_costdtl.CHARGETYPE = 13 THEN '草药' ELSE '西药' END prescriptionType, op_costdtl.CHARGENAME drug, op_costdtl.SPEC specifications, op_costdtl.QUANTITY singleDose, op_costdtl.CALCUNIT unit, op_costdtl.DOSE traditionalChineseMedicineNumber, op_costdtl.QUANTITY * op_costdtl.DOSE totalDrugDose, op_recipe.CREATETIME prescriptionDate FROM op_recipe, op_register, ( SELECT * FROM SBAS_EMPLOYEE GROUP BY OURID ) SBAS_EMPLOYEE, op_costdtl WHERE op_register.OURID = op_recipe.REGISTERID AND SBAS_EMPLOYEE.OURID = op_recipe.CREATORID AND op_costdtl.RECIPEID = op_recipe.OURID AND op_costdtl.CHARGETYPE IN (11, 12, 13) AND op_costdtl.PARENTSERIAL != -1", columnsList, null,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 inputer.setQueueSize(100000);
		
		 outputer =new JDBCOutputer("outp_drug",columnsList,"jdbc:mysql://172.16.40.125:3306/db_hospital_20160917?useUnicode=true&characterEncoding=utf8","root","SXAD13579@$^*)",JDBC_DRIVER.MYSQL.getValue());
		 outputer.setSubmitSize(100000);
			
			
		 inputer.out(outputer);
		
		
	}
	
	
	
	
	
	public static void main(String[] args) {

//		完整病历2导入.病人基本信息();
		
		
//		完整病历2导入.op_register();
//		
//		
//		完整病历2导入.op_diagnose();
//		 
//		完整病历2导入.op_recipe();
//		完整病历2导入.op_da(); 
		
		
//		完整病历2导入.	op_costdtl();
		
//		完整病历2导入 .住院诊断();
		//完整病历2导入.ip_checkin();
		//完整病历2导入.ip_advice();
		
		//完整病历2导入.testReport_Items();
		
//		完整病历2导入.import_to_hosp_diagnosis();
		
		//完整病历2导入.import_to_hosp_drug();
		
		
//		完整病历2导入.import_to_hosp_test();
		
		
//		完整病历2导入.import_to_outp_diagnosis();
//		完整病历2导入.import_to_outp_test();
		
		完整病历2导入.import_to_outp_drug();
	}
	
	
	

}
