package com.seassoon.common.config;

import com.jfinal.config.Constants;
import com.jfinal.config.Handlers;
import com.jfinal.config.Interceptors;
import com.jfinal.core.JFinal;
import com.jfinal.kit.PropKit;
import com.jfinal.plugin.activerecord.ActiveRecordPlugin;
import com.jfinal.plugin.c3p0.C3p0Plugin;
import com.jfinal.plugin.druid.DruidPlugin;
import com.seassoon.common.model._MappingKit;

/**
 * API引导式配置
 */

public class JfinalORMConfig {
	
	static{
		PropKit.use("jdbc.properties");
		configPlugin();
	}
	
	
	/**
	 * 配置常量
	 */
	public void configConstant(Constants me) {
		// 加载少量必要配置，随后可用PropKit.get(...)获取值
		PropKit.use("jdbc.properties");
		me.setDevMode(PropKit.getBoolean("devMode", false));
	}
	

	public static C3p0Plugin createC3p0Plugin() {
		return new C3p0Plugin(PropKit.get("url"), PropKit.get("user"), PropKit.get("password").trim());
	}
	public static DruidPlugin createDruidPlugin() {
		return new DruidPlugin(PropKit.get("url"), PropKit.get("user"), PropKit.get("password").trim());
	}
	/**
	 * 配置插件
	 */
	public static  void configPlugin() {
		// 配置C3p0数据库连接池插件
//		DruidPlugin C3p0Plugin = createDruidPlugin();
		C3p0Plugin C3p0Plugin = createC3p0Plugin();
		C3p0Plugin.start();
		
		// 配置ActiveRecord插件
		ActiveRecordPlugin arp = new ActiveRecordPlugin(C3p0Plugin);
		
		
		// 所有配置在 MappingKit 中搞定
		_MappingKit.mapping(arp);
		
		arp.start();
	}
	
	/**
	 * 配置全局拦截器
	 */
	public void configInterceptor(Interceptors me) {
		
	}
	
	/**
	 * 配置处理器
	 */
	public void configHandler(Handlers me) {
		
	}
	
	/**
	 * 建议使用 JFinal 手册推荐的方式启动项目
	 * 运行此 main 方法可以启动项目，此main方法可以放置在任意的Class类定义中，不一定要放于此
	 */
	public static void main(String[] args) {
		JFinal.start("WebRoot", 80, "/", 5);
	}
}
