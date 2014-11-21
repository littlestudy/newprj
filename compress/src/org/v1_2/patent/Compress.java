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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.v1_2.patent.IOformat.NoSeperatorTextOutputFormate;

public class Compress {

	public static class MapperClass extends
			Mapper<LongWritable, Text, Text, Text> {
		public static final String separator = "##";

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String subStr = "";

			if (line.contains("[")) {
				subStr = line.substring(line.indexOf("["));
				line = line.substring(0, line.indexOf("["));
			}

			if (!line.contains(separator)) { // 对于第一个字段组的情况
				context.write(new Text(""), new Text("(" + line + ")" + subStr));
				return;
			}

			int lastindex = line.lastIndexOf(separator);
			String newKey = line.substring(0, lastindex);
			String newValue = "("
					+ line.substring(lastindex + separator.length()) + ")";
			context.write(new Text(newKey), new Text(newValue + subStr));
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				sb.append(value.toString());
			}
			if (key.toString().equals(""))
				context.write(key, new Text(sb.toString()));
			else
				context.write(key, new Text("[" + sb.toString() + "]"));
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "file:///home/ym/ytmp/data/test3";
		String outputbase = "file:///home/ym/ytmp/data/output/newtest--";
		runJob(input, outputbase);
	}

	private static void runJob(String input, String outputbase) throws Exception {
		Configuration conf = new Configuration();
		Job job = null;

		String output = outputbase;
		for (int i = 0; i < getGroups2().size(); i++) {

			job = new Job(conf);
			initJob(job);
			setJobPath(job, input, output);
			job.waitForCompletion(true);
			input = output;
			output = outputbase + i;
		}
	}

	public static void setJobPath(Job job, String input, String output)
			throws Exception {
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
	}

	public static void initJob(Job job) throws Exception {

		job.setJarByClass(Compress.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NoSeperatorTextOutputFormate.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}

	public static List<String[]> getGroups() {
		List<String[]> groups = new ArrayList<String[]>();
		String[] group1 = { "appKey", "appVersion", "dataType" };
		String[] group2 = { "city", "ip", "isp", "logCity", "logProvince" };
		String[] group3 = { "deviceCarrier", "deviceHashMac", "deviceIMEI",
				"deviceMacAddr", "deviceModel", "deviceNetwork", "deviceOs",
				"deviceOsVersion", "deviceResolution", "deviceUdid",
				"appChannel" };
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
	
		public static List<String[]> getGroups2() {
		List<String[]> groups = new ArrayList<String[]>();
		String[] group1 = { "field1", "field2" };
		String[] group2 = { "field3", "field4" };
		String[] group3 = { "field5", "field6" };

		groups.add(group1);
		groups.add(group2);
		groups.add(group3);
		
		return groups;
	}
}
