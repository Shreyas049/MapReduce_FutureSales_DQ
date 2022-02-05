package com.demo.Miniproject_FutureSales_DQ;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DQ_Job extends Configured implements Tool {
	
	/*
	 * Perform the following operations in Mapreduce ( Using Java ) 
	 * 1. Find the slowest train that arrives at the station per direction 
	 * 2. Find the train that spends min time at the station 
	 * 3. Aggregate data of all the trains that are coming from each direction in a HDFS file. For e.g. All trains coming from EAST, will be
	 * stored in one file in HDFS. Similar for WEST, NORTH and SOUTH 
	 * 4. Needless to say, if there is any garbage data into the data, that should be filtered out.
	 * 5. Keep a counter of garbage data that you encounter during the process 
	 * 6.  Find the count of trains coming in each direction.
	 */
	
	public static void main(String[] args) throws Exception {
		Tool job = new DQ_Job();
		int exitcode = ToolRunner.run(job, args);
		
		System.exit(exitcode);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// This is the input argument coming from hadoop jar command
		String inPath = args[0];
		String outPath = args[1];

		// This is the job instance that will initiate the job
		Job job = Job.getInstance();
		// We need to register the job class with the job in order to let job find the class
		job.setJarByClass(DQ_Job.class);
		// This is how we set the job name, this will come up on the YARN WebUI console
		job.setJobName("miniproject_DQ_Job");
		
		// We need to set the mapper class and the reducer class as below for this particular job
		job.setMapperClass(DQ_Mapper.class);
		//job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(DQ_Reducer.class);
		
		// For the mapper and reducer, we need to specify the type of key and value we will be using
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// We also need to provide the output format of the job, this is the format Reducer will use to write 
		// onto HDFS
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set the input location and the output location as received from the 
		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		
		int returnValue = job.waitForCompletion(true) ? 0:1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		return returnValue;	
	}
	
}
