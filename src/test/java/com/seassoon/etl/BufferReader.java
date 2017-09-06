package com.seassoon.etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import net.sf.json.JSONObject;

public class BufferReader {

	
	public static void main(String[] args) {
		

		File file = new File("I:\\工作目录\\中医院\\3-检验信息\\3-检验信息\\View_XKY_TestReport_Items(2).csv");

		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new InputStreamReader( new FileInputStream(file)));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String line = null;

		

		try {
			while ((line = bufferedReader.readLine()) != null) {

					System.out.println(line);
			}
			
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
