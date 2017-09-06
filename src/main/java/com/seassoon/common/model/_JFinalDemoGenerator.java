package com.seassoon.common.model;


import javax.sql.DataSource;

import com.jfinal.kit.PathKit;
import com.jfinal.kit.PropKit;
import com.jfinal.plugin.activerecord.generator.Generator;
import com.jfinal.plugin.c3p0.C3p0Plugin;
import com.seassoon.common.config.JfinalORMConfig;


/**
 * 在数据库表有任何变动时，运行一下 main 方法，极速响应变化进行代码重构
 */
public class _JFinalDemoGenerator {
	
	public static DataSource getDataSource() {
		PropKit.use("jdbc.properties");
//		DruidPlugin c3p0Plugin = JfinalORMConfig.createDruidPlugin();
		C3p0Plugin c3p0Plugin = JfinalORMConfig.createC3p0Plugin();
		c3p0Plugin.start();
		return c3p0Plugin.getDataSource();
	}
	
	public static void main(String[] args) {
		
		getDataSource();
//		List<Record> list = Db.query("select * from test");
//		for (Object object : list) {
//			System.out.println(object);
//		}
		// base model 所使用的包名
		String baseModelPackageName = "com.seassoon.common.model.base";
		// base model 文件保存路径
		String baseModelOutputDir = PathKit.getWebRootPath() + "/src/main/java/com/seassoon/common/model/base";
		
	 
		
		// model 所使用的包名 (MappingKit 默认使用的包名)
		String modelPackageName = "com.seassoon.common.model";
		// model 文件保存路径 (MappingKit 与 DataDictionary 文件默认保存路径)
		String modelOutputDir = baseModelOutputDir + "/..";
		
		// 创建生成器
		Generator gernerator = new Generator(getDataSource(), baseModelPackageName, baseModelOutputDir, modelPackageName, modelOutputDir);
		// 添加不需要生成的表名
		gernerator.addExcludedTable("adv");
		gernerator.addExcludedTable("按病人ID时间合并诊断");
		gernerator.addExcludedTable("包含两次诊断的病人");
		// 设置是否在 Model 中生成 dao 对象
		gernerator.setGenerateDaoInModel(true);
		// 设置是否生成字典文件
		gernerator.setGenerateDataDictionary(false);
		// 设置需要被移除的表名前缀用于生成modelName。例如表名 "osc_user"，移除前缀 "osc_"后生成的model名为 "User"而非 OscUser
		gernerator.setRemovedTableNamePrefixes("sc_");
		// 生成
		gernerator.generate();
	}
}

 


