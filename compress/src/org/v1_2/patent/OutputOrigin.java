package org.v1_2.patent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.v1_2.patent.IOformat.JsonToCsvInputForamt;
import org.v1_2.patent.IOformat.NoSeperatorTextOutputFormate;

public class OutputOrigin {
	public static class MappClass extends Mapper<LongWritable, Text, Text, Text> {
		private Text okey = new Text();
		private Text ovalue = new Text();
		private long size = 0;
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().replaceAll("##", ",");
			size += line.getBytes().length;
			okey.set(line);
			context.write(okey, ovalue);
		}
		
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("  size -- " + size);
		}
	}
	
	public static class ReduceClass extends
			Reducer<Text, Text, Text, Text> {
		private Text ovalue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			context.write(key, ovalue);
		}
	}
	
	public static void main(String[] args) throws Exception {
				String input = "/home/ym/ytmp/data/statistics/1mRsample";		
		String output = "/home/ym/ytmp/data/statistics/output/OutputOrigin3";
		runJob(input, output);
	}
	
		private static void runJob(String input, String output) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(OutputOrigin.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);

		JsonToCsvInputForamt.setGroups(getGroups());
		job.setInputFormatClass(JsonToCsvInputForamt.class);
		job.setOutputFormatClass(NoSeperatorTextOutputFormate.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path outputPath = new Path(output);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
	}
		
	public static List<String[]> getGroups(){
			List<String[]> groups = new ArrayList<String[]>();
			String[] group1 = { "appKey", "appVersion", "dataType" };
			String[] group2 = { "city", "ip", "isp", "logCity", "logProvince" };
			String[] group3 = { "deviceCarrier", "deviceHashMac", "deviceIMEI",
					"deviceMacAddr", "deviceModel", "deviceNetwork",
					"deviceOs", "deviceOsVersion", "deviceResolution",
					"deviceUdid", "appChannel" };
			String[] group4 = { "userName" };
			String[] group5 = { "occurTime", "persistedTime" };
			String[] group6 = { "eventId", "costTime", "logSource" };
			String[] group7 = { "sessionStep" };
			groups.add(group1);
			groups.add(group2);
			groups.add(group3);
			groups.add(group4);
			groups.add(group5);
			groups.add(group6);
			groups.add(group7);
			
			return groups;
	}
}
