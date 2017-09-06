package com.seassoon.etl;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.constraint.LMinMax;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.constraint.StrRegEx;
import org.supercsv.cellprocessor.constraint.UniqueHashCode;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

public class SuperCsv {

	/**
	 * An example of reading using CsvListReader.
	 */
	private static void readWithCsvListReader() throws Exception {
	        
//	        ICsvListReader listReader = null;
//	        try {
//	                listReader = new CsvListReader(new FileReader("I:\\工作目录\\中医院\\5-医嘱\\ip_advice2012-2015.csv"), CsvPreference.STANDARD_PREFERENCE);
//	                
//	                listReader.getHeader(true); // skip the header (can't be used with CsvListReader)
//	                final CellProcessor[] processors = getProcessors();
//	                
//	                List<Object> customerList;
//	                while( (customerList = listReader.read(processors)) != null ) {
//	                        System.out.println(String.format("lineNo=%s, rowNo=%s, customerList=%s", listReader.getLineNumber(),
//	                                listReader.getRowNumber(), customerList));
//	                }
//	                
//	        }
//	        finally {
//	                if( listReader != null ) {
//	                        listReader.close();
//	                }
//	        }
	}
private static CellProcessor[] getProcessors() {
        
        final String emailRegex = "[a-z0-9\\._]+@[a-z0-9\\.]+"; // just an example, not very robust!
        StrRegEx.registerMessage(emailRegex, "must be a valid email address");
        
        final CellProcessor[] processors = new CellProcessor[] { 
                new UniqueHashCode(), // customerNo (must be unique)
                new NotNull(), // firstName
                new NotNull(), // lastName
                new ParseDate("dd/MM/yyyy"), // birthDate
                new NotNull(), // mailingAddress
                new Optional(new ParseBool()), // married
                new Optional(new ParseInt()), // numberOfKids
                new NotNull(), // favouriteQuote
                new StrRegEx(emailRegex), // email
                new LMinMax(0L, LMinMax.MAX_LONG) // loyaltyPoints
        };
        
        return processors;}
	
	public static void main(String[] args) throws IOException {

        ICsvListReader listReader = null;
        try {
                listReader = new CsvListReader(new FileReader("I:\\工作目录\\中医院\\5-医嘱\\ip_advice2012-2015.csv"), CsvPreference.STANDARD_PREFERENCE);
                
             //   listReader.getHeader(true); // skip the header (can't be used with CsvListReader)
                final String[] header = listReader.getHeader(true);
                final CellProcessor[] processors = new CellProcessor[header.length];
                
                List<Object> customerList;
                while( (customerList = listReader.read(processors)) != null ) {
                        System.out.println(String.format("lineNo=%s, rowNo=%s, customerList=%s", listReader.getLineNumber(),
                                listReader.getRowNumber(), customerList));
                }
                
        }
        finally {
                if( listReader != null ) {
                        listReader.close();
                }
        }

	}

}
