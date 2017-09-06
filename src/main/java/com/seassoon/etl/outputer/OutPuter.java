package com.seassoon.etl.outputer;

public interface OutPuter {

	
	public void process(String[] data);
	
	
	public void close();
}
