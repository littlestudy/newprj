package org.compress;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.utils.EJob;

public class Compress {
	private static final Logger log = LoggerFactory.getLogger(Compress.class);
	private static int position = 0;
	
	public static class MappClass extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();			
			String subStr = "";
			
			if (line.contains("[")){
				subStr = line.substring(line.indexOf("["));				
				line = line.substring(0, line.indexOf("[") - 1);				
			}
			
			if (position == 0){ //对于第一个字段组的情况
				context.write(new Text(""), new Text("(" + line + ")" + subStr));
				return;
			}
			
			int prepos = 0;
			int curpos = 0;
			for (int i = 0; i < position; i++){
				curpos = line.indexOf(",", prepos + 1);
				prepos = curpos;
			}
			String newKey = line.substring(0, prepos);
			String newValue = "(" + line.substring(prepos + 1) + ")";			
			context.write(new Text(newKey), new Text(newValue + subStr));
		}
	}
	
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text value : values){
				sb.append(value.toString());
			}
			if (key.toString().equals(""))
				context.write(key, new Text(sb.toString()));
			else
				context.write(key, new Text("[" + sb.toString() + "]"));
		}
	}
	
	public static void setJobPath(Job job, String input, String output) throws Exception{
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
	}
	
	public static void initJob(Job job) throws Exception{
		File jarFile = EJob.createTempJar("bin");
        //EJob.addClasspath("/usr/lib/hadoop-0.20/conf");
        ClassLoader classLoader = EJob.getClassLoader();        
        Thread.currentThread().setContextClassLoader(classLoader);
		((JobConf) job.getConfiguration()).setJar(jarFile.toString());
		
		job.setJarByClass(Compress.class);
		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
	
	public static void main(String[] args) throws Exception {
		

		String input = "hdfs://192.168.195.128:9000/user/hadoop/test";
		String output = "hdfs://192.168.195.128:9000/user/hadoop/aa";
		
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "192.168.195.128:9001"); 
		//conf.set("mapred.textoutputformat.separator", ",");
		//conf.set("mapred.textoutputformat.ignoreseparator", "true");
		
		Job job = new Job(conf);
		initJob(job);
		
		//position = 4;
		//setJobPath(job, input, output + "1");
		//setJobPath(job, input, output + "1");
		//setJobPath(job, output + "1", output + "2");
		//setJobPath(job, output + "2", output + "3");
		setJobPath(job, output + "3", output + "4");
		job.waitForCompletion(true);
		
		
		/*
		position = 2;
		setJobPath(job, output + "1", output + "2");
		job.waitForCompletion(true);
		
		position = 1;
		setJobPath(job, output + "2", output + "");
		job.waitForCompletion(true);
		
		position = 0;
		setJobPath(job, output + "3", output + "4");
		job.waitForCompletion(true);
		*/
	}
}
