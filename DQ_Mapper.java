package com.demo.Miniproject_FutureSales_DQ;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class DQ_Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	// 1.Check negative values in last column
	// 2.To separate date into year, month, day columns
	// 3.Check for missing values
	
	// Function to check if the string can be converted to numeric data
	public static boolean isNumeric(String string) {
	    int intValue;
			
	    if(string == null || string.equals("") || string.strip().equals("\"\"")) {
	        return false;
	    }
	    
	    try {
	        intValue = Integer.parseInt(string);
	        return true;
	    } catch (NumberFormatException e) {
	        System.out.println("Input String cannot be parsed to Integer.");
	    }
	    return false;
	}
	// Function to check missing values in data
	public static boolean checkMissing(String[] splittedString) {
		for(String temp: splittedString) {
			if(temp.equals(""))
				return true;
			else 
				return false;
		}
		return true;
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// For keeping the track of whether or not header is written in context
		int headerFlag = 0;
		
		String line = value.toString();
		// line -> date,date_block_num,shop_id,item_id,item_price,item_cnt_day
		
		// Creating an array of strings that contains all the fields in csv file
		String[] splittedString = line.split("\\,");
		
		// For rows other than header splittedString[0] will be 'dd.mm.yyyy'
		String[] date = splittedString[0].strip().split("\\.");
		
		// For header line
		if(splittedString[0].strip().equalsIgnoreCase("date")) {
			headerFlag = 1;										
			context.write(new Text("0"), new Text(splittedString[0] + "," + splittedString[1] + "," + splittedString[2]
					+ "," + splittedString[3] + "," + splittedString[4] + "," + splittedString[5]));
		}
		
		int missingValuesCnt = 0;
		int negativeValuesCnt = 0;
		int faultyDatesCnt = 0;
		
		if(checkMissing(splittedString)) {
			missingValuesCnt = 1;
			context.getCounter("RuntimeStats","GarbageDataMissingValues").increment(1);
		}
		else if(!splittedString[5].strip().equalsIgnoreCase("item_cnt_day") && Float.parseFloat(splittedString[5].strip()) < 0 ) {
			negativeValuesCnt = 1;
			context.getCounter("RuntimeStats","GarbageDataNegativeValues").increment(1);
		}
		else if(!date[0].strip().equalsIgnoreCase("date") && (0 > Integer.parseInt(date[0]) || Integer.parseInt(date[0]) >= 31 || 0 > Integer.parseInt(date[1]) || Integer.parseInt(date[1]) >= 12))  {
			faultyDatesCnt = 1;
			context.getCounter("RuntimeStats","GarbageDataDateValues").increment(1);
		}
		
		// Giving key as year values to reducer(will run only if line is not header)
		if(headerFlag == 0 && missingValuesCnt == 0 && negativeValuesCnt == 0 && faultyDatesCnt == 0) {
			context.write(new Text(date[2]), new Text(splittedString[0] + "," + splittedString[1] + "," 
					+ splittedString[2] + "," + splittedString[3] + "," + splittedString[4] + "," + splittedString[5]));
		}
					
		System.out.println(missingValuesCnt);
		System.out.println(negativeValuesCnt);
		System.out.println(faultyDatesCnt);
		
	}
	
}