package com.seassoon.etl.input;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.seassoon.etl.outputer.OutPuter;

public class AbstractInputer {

	protected BlockingQueue<String[]> queue;

	protected List<String> originalColumnsList;

	
	
	//自定义虚输出的列  默认取第一行为列头
	protected List<String> columnsList;

	
	//转换列  key 原始列名  value 为新列名
	protected Map<String, String> columnConvertMap;
	
	protected Thread initThread;
	
	//存放读取队列大小
	protected int queueSize = 100000;
	
	//，++i和i++操作并不是线程安全的，在使用的时候，不可避免的会用到synchronized关键字。而AtomicInteger则通过一种线程安全的加减操作接口
	public AtomicInteger inputNum=new AtomicInteger(0);
	
	public AtomicInteger outNum=new AtomicInteger(0);

	public Thread getInitThread() {
		return initThread;
	}

	public void setInitThread(Thread initThread) {
		this.initThread = initThread;
	}

	public int getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public String[] next() {
		while (true) {
			String[] data = queue.poll();//取出队列的第一个元素
			outNum.incrementAndGet();
			if (data != null) {
				return data;
			} else if (!initThread.isAlive() && data == null) {
				return null;
			}
		
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	
	protected void columnConvert() {
		
		
		if (columnConvertMap != null && columnConvertMap.size() > 0) {

			for (int i = 0; i < originalColumnsList.size(); i++) {
				if (columnConvertMap.get(originalColumnsList.get(i)) != null) {
					originalColumnsList.set(i, columnConvertMap.get(originalColumnsList.get(i)));
				}

			}

		}
		
		for (int i = 0; i < originalColumnsList.size(); i++) {
			if(originalColumnsList.indexOf(originalColumnsList.get(i))<i){
				//data[i]=data[i]+"1";
				
				originalColumnsList.set(i, originalColumnsList.get(i)+"1");
			}
		}

	}

	
	public void out(OutPuter outPuter) {
		String[] data = null;
		while (true) {
			data = this.next();

			if (data == null) {
		
				outPuter.close();
				
				break;
			}

			outPuter.process(data);
		}
	}
}
