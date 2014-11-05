package org.v1_2.test.jsontocsv;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.v1_2.compress.JsonToCsvInputForamt;

public class Main {

	public static class MappClass extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, new Text(""));
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}

	public static void setJobPath(Job job, String input, String output)
			throws Exception {
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
	}

	public static void initJob(Job job) throws Exception {

		job.setJarByClass(Main.class);
		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);
		MyInputFormat.getTargetFileds(); //
		job.setInputFormatClass(MyInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}

	public static void main(String[] args) throws Exception {
		String input = "file:///home/ym/ytmp/data/test2";
		String output = "file:///home/ym/ytmp/data/output/test2";
		Configuration conf = new Configuration();
		conf.set("dfs.permissions", "false");
		Job job = new Job(conf);
		initJob(job);
		setJobPath(job, input, output);
		job.waitForCompletion(true);
	}
}
