package com.seassoon.etl.outputer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.seassoon.etl.outputer.impl.MysqlOutputer;
import com.seassoon.utils.DbUtils;

public class AbstractOutPuter  {

	public BlockingQueue<String[]> queue;


	public List<String> columnsList;



	private List<String[]> list = new ArrayList<>();



	
	public AtomicInteger processNum=new AtomicInteger(0);
	
	
	
}
