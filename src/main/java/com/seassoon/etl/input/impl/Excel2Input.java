package com.seassoon.etl.input.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.xml.sax.SAXException;

import com.seassoon.etl.input.AbstractInputer;
import com.seassoon.etl.input.Inputer;
import com.seassoon.utils.XLSXCovertCSVReader;

	

public class Excel2Input extends AbstractInputer implements Inputer {

	private String filePath;
	private String sheetName;
	private int minColumns;	
	private Boolean isFirstHead = true;

	public Excel2Input(String filePath, String sheetName, int minColumns, List<String> columnsList,
			Map<String, String> columnConvertMap) {
		super();
		this.filePath = filePath;
		this.sheetName = sheetName;
		this.columnsList = columnsList;
		this.minColumns = minColumns;
		this.columnConvertMap = columnConvertMap;
		init();
	}

	private void init() {

		if (queue == null) {
			queue = new LinkedBlockingQueue<>(queueSize);
		}
	

		try {
			final List<String[]> list = XLSXCovertCSVReader.readerExcel(filePath, sheetName, minColumns);

			initThread = new Thread(new Runnable() {

				@Override
				public void run() {
					int currentCount = 0;
					String[] tempData = null;
					for (String[] data : list) {

						currentCount++;

						try {
							if (isFirstHead == true && currentCount == 1) {

								originalColumnsList = Arrays.asList(data);

								columnConvert();

							} else {

								if (columnsList == null || columnsList.size() == 0) {

									//columnsList = originalColumnsList;
									for (int i = 0; i < data.length; i++) {
										columnsList.add(originalColumnsList.get(i));
									}
								}

								tempData = new String[columnsList.size()];

								for (int i = 0; i < originalColumnsList.size(); i++) {

									// if (columnsList == null ||
									// columnsList.size() == 0) {
									int columnIndex = columnsList.indexOf(originalColumnsList.get(i));
									if (columnIndex >= 0) {
										tempData[columnIndex] = data[i];
									}
									// } else {
									// tempData[i] = data[i];
									// }

								}
								queue.put(tempData);
							}

						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

				}
			});
			initThread.start();
		} catch (IOException | OpenXML4JException | ParserConfigurationException | SAXException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	// @Override
	// public String[] next() {
	// while (initThread.isAlive()) {
	// String[] data = queue.poll();
	// if (data != null) {
	// return data;
	// }
	//
	// try {
	// Thread.sleep(5000);
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// return null;
	// }

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

}
