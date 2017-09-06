package com.seassoon.jinhua.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.hssf.record.DBCellRecord;

import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.mchange.v1.util.ArrayUtils;
import com.seassoon.common.config.JfinalORMConfig;
import com.seassoon.etl.input.impl.MysqlInputer;
import com.seassoon.etl.outputer.impl.MysqlOutputer;

public class Judgement_ZSCQ {

	public static List<String> list = new ArrayList<>();

	public static Map<String, Set<String>> map = new HashMap<String, Set<String>>();

	public static void addCivilLiability(String type, String case_num) {

		Set<String> set = map.get(type);
		if (set == null) {
			set = new HashSet<String>();
		}
		set.add(case_num);
		map.put(type, set);
	}

	public static void extractResponsibility(String content, String case_num) {

		// Pattern p = Pattern.compile("。.*?赔偿损失.*?。");
		// Matcher matcher = p.matcher(content);
		//
		// while (matcher.find()) {
		//
		// System.out.println(matcher.group());
		//
		// }

		content = content.replace("&amp;#xA;", " ");
		if (content.contains("判决如下")) {
			content = content.substring(content.indexOf("判决如下"));
		}

		String[] temp = content.split("。");

		if (temp.length == 1) {
			System.out.println();
		}

		String[] trueWords = new String[] { "承担", "予以支持", "应予支持", "予以准许", "应予准许", "立即停止侵权", "赔偿" };
		String[] falseWords = new String[] { "不予支持", "不予以支持", "不予准许", "不应承担" };
		String[] ignoreWords = new String[] { "有下列侵权行为的，应当根据情况，承担停止侵害、消除影响、赔礼道歉、赔偿损失等民事责任", "请求法院判令", "请求法院依法判令",
				"请求法院判令被告", "请求依法判令", "请求判令被告", "请判令被告", "请求判令", "原告请求：", "判令", "请求人民法院判决", "被告辩称：", "法院判令", "依法判令",
				"请求依法判处", "《中华人民共和国民法通则》第一百一十八条规定公民、法人的商标专用权受到侵害的，有权要求停止侵害，消除影响，赔偿损失",
				"公民、法人的商标专用权受到侵害的，权利人有权要求停止侵害，消除影响，赔偿损失", "，根据履行情况和合同性质，当事人可以要求恢复原状、采取其他补救措施，并有权要求赔偿损失", "中华人民共和国民法通则",
				"请求判决", "有下列侵权行为的，应当根据情况，承担停止侵害、消除影响、赔礼道歉、赔偿损失等民事责任", "应当承担继续履行、采取补救措施或者赔偿损失等违约责任", "承担侵权责任的方式主要有",
				"依据民法通则第一百三十四条", "承担民事责任的方式主要有", "有权要求停止侵害，消除影响，赔偿损失", "可以判决侵权人承担停止侵害、排除妨碍、消除危险、赔偿损失、消除影响等民事责任",
				"有权要求停止侵害、消除影响、赔偿损失" };

		// String[] ignoreWords = new String[] {
		// "有下列侵权行为的，应当根据情况，承担停止侵害、消除影响、赔礼道歉、赔偿损失等民事责任",
		// "应当承担继续履行、采取补救措施或者赔偿损失等违约责任","承担侵权责任的方式主要有","依据民法通则第一百三十四条","承担民事责任的方式主要有","有权要求停止侵害，消除影响，赔偿损失","可以判决侵权人承担停止侵害、排除妨碍、消除危险、赔偿损失、消除影响等民事责任","有权要求停止侵害、消除影响、赔偿损失"
		// };

		int count = 0;

		if (case_num.contains("（2013）江宁知民初字第127号")) {
			System.out.println();
		}
		for (String string : temp) {

			if ((Pattern.compile("立即(.*?)停止").matcher(string).find()) && !isExists(string, ignoreWords)) {

				if (Judgement_ZSCQ.isExists(string, trueWords)) {

//					System.out.println("start-------------------");
//					System.out.println(string);
//					System.out.println("end-------------------");
					addCivilLiability("停止侵权", case_num);
					break;
				}
			}

		}

		for (String string : temp) {

			if ((Pattern.compile("赔偿(.*?)损失").matcher(string).find()) && !isExists(string, ignoreWords)) {

				if (Judgement_ZSCQ.isExists(string, trueWords) && !Judgement_ZSCQ.isExists(string, falseWords)) {

					// System.out.println("start-------------------");
//					System.out.println(case_num + "----" + string);
					// System.out.println("end-------------------");
					// list.add(case_num);
					addCivilLiability("赔偿损失", case_num);
					break;
				}
			}

		}
		for (String string : temp) {

			if ((Pattern.compile("消除(.*?)影响").matcher(string).find()) && !isExists(string, ignoreWords)) {

				if (Judgement_ZSCQ.isExists(string, trueWords) && !Judgement_ZSCQ.isExists(string, falseWords)) {

					// System.out.println("start-------------------");
//					System.out.println(case_num + "----" + string);
					// System.out.println("end-------------------");
					// list.add(case_num);
					addCivilLiability("消除影响", case_num);
					break;
				}
			}

		}
		for (String string : temp) {

			if ((Pattern.compile("赔礼.*?道歉").matcher(string).find()) && !isExists(string, ignoreWords)) {

				if (Judgement_ZSCQ.isExists(string, trueWords) && !Judgement_ZSCQ.isExists(string, falseWords)) {

					// System.out.println("start-------------------");
//					System.out.println(case_num + "----" + string);
					// System.out.println("end-------------------");
					// list.add(case_num);
					addCivilLiability("赔礼道歉", case_num);
					break;
				}
			}

		}

	}

	public static Boolean isExists(String content, String[] keywords) {

		// boolean result= false;

		for (String string : keywords) {

			if (content.contains(string)) {
				return true;
			}
		}

		return false;

	}

	public static void main(String[] args) {

		JfinalORMConfig jfinalORMConfig = new JfinalORMConfig();

		final List<String> columnsList = new ArrayList<>();
		final List<String> columnsList2 = Arrays.asList(new String[] { "case_num", "type" });

		MysqlInputer inputer = new MysqlInputer("select * from judgement_ZSCQ", columnsList, null);

		String[] data = null;
		int count = 0;
		while (true) {
			data = inputer.next();

			if (data == null) {
				// outPuter.process(data);
				break;
			}

			Object doc_content = data[2];

			// System.out.println(doc_content);

			// System.out.println("start@@@@@@@@@@@@@@@" + data[7] + " " +
			// data[21]);
			Judgement_ZSCQ.extractResponsibility(doc_content.toString(), data[7].toString());
			// System.out.println("end@@@@@@@@@@@@@@@");
			// outPuter.process(data);
		}

		MysqlOutputer outputer = new MysqlOutputer("judgement_civil_liability", columnsList2);

		Iterator<Entry<String, Set<String>>> iterator = map.entrySet().iterator();

		// Iterator<Map.Entry<String, Integer>> iterator =
		// map.entrySet().iterator();

		// for (Map.Entry<String, Integer> entry : map.entrySet()) {
		Entry<String, Set<String>> entry = null;
		// Object[] data = null;
		while (true) {
			if (!iterator.hasNext()) {
				outputer.process(null);
				break;
			} else {
				entry = iterator.next();
			}
			// for (Entry<String, Set<String>> entry : map.entrySet()) {

			System.out.println(entry.getKey() + "：" + entry.getValue().size());

			for (String string : entry.getValue()) {
				data = new String[columnsList2.size()];
				data[0] = string;
				data[1] = entry.getKey();

				outputer.process(data);
			}

		}

	}

}
