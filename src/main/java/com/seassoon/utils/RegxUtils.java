package com.seassoon.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegxUtils {

	
	public static String getMatcher(String regex, String source) {
		String result = "";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(source);
		while (matcher.find()) {
			
			result = matcher.group(1);
		}
		return result;
	}
	public static String getMatcherGroup(String regex, String source) {
		String result = "";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(source);
		while (matcher.find()) {
			
			result = matcher.group();
		}
		return result;
	}
}
