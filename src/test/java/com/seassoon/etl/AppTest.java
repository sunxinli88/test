package com.seassoon.etl;

import java.util.List;

import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.seassoon.common.config.JfinalORMConfig;
import com.seassoon.etl.input.impl.JDBCInputer;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
        JfinalORMConfig config=new JfinalORMConfig();
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
    	
    	
    	
    	//JDBCInputer jdbcInputer =new JDBCInputer(sql, columnsList, columnConvertMap) 
    	
    	List<Record>  list=  Db.find("DESCRIBE judgement_ZSCQ");
    	
    	for (Record record : list) {
			System.out.println(record.getStr("Field"));
		}
    	
    	
    	
        //assertTrue( true );
    }
}
