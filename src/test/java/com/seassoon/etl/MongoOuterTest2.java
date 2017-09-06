package com.seassoon.etl;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.seassoon.utils.MongoDBUtils;

public class MongoOuterTest2 {
	
	
	public static void main(String[] args) {
		
	
		MongoDBUtils mongoDBUtils =MongoDBUtils.getInstance("172.16.40.122", 28117);
		
		final MongoCollection a2=  mongoDBUtils.getCollection("suichao", "a2");
		
		MongoCursor<Document> mongoCursor = a2.find().iterator();
		
		ThreadPoolExecutor executor =new ThreadPoolExecutor(1, 1, 10000, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		
		int i =0;
		while (mongoCursor.hasNext()) {
			i++;
		 	Document document= mongoCursor.next();
			
//			a2.withWriteConcern(WriteConcern.UNACKNOWLEDGED).updateOne(Filters.eq("_id", document.get("_id")), new Document("$set",  new Document("PATIID",document.get("PATIID"))));
			
			final Document documentFinal=document;
			
			executor.execute(new Runnable() {
				
				@Override
				public void run() {
					a2.withWriteConcern(WriteConcern.UNACKNOWLEDGED).updateOne(Filters.eq("_id", documentFinal.get("_id")), new Document("$set",  new Document("PATIID",documentFinal.get("PATIID"))));
					
					
					
				}
			});
			
		
			//System.out.println(i);
		//	type type = (type) mongoCursor.nextElement();
			
		}
		executor.shutdown();
		
	}

}
