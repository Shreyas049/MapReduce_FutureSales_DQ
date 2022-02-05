package com.demo.Miniproject_FutureSales_DQ;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DQ_Reducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		System.out.println("Inside Reducer");
		Iterator<Text> valuesIt = values.iterator();
		
		// Splitting the date header
		if(key.toString().equalsIgnoreCase("0")) {
			System.out.println("Header's if");
			
			String line = valuesIt.next().toString();
			String[] splittedString = line.split("\\,");
			
			context.write(new Text("year,month,day," + splittedString[1] + "," + splittedString[2]
					+ "," + splittedString[3] + "," + splittedString[4] + "," + splittedString[5] + ","), new Text(""));
		}
		else {
			while(valuesIt.hasNext()) {
				
				String line = valuesIt.next().toString();
				
				String[] splittedString = line.split("\\,");
				
				String[] date = splittedString[0].split("\\.");
				
				int date_block_num = Integer.parseInt(splittedString[1]);
				int shop_id = Integer.parseInt(splittedString[2]);
				int item_id = Integer.parseInt(splittedString[3]);
				float item_price = Float.parseFloat(splittedString[4]);
				int item_cnt_day = (int)Float.parseFloat(splittedString[5]);
				
				context.write(new Text(date[2] + "," + date[1] + "," + date[0] + "," + date_block_num + "," + shop_id 
						+ "," + item_id + "," + item_price + "," + item_cnt_day + ","), new Text(""));
			}
		}
		
		
	}
}
