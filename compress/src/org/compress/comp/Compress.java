package org.compress.comp;

import java.io.IOException;

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
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Compress {
	private static final Logger log = LoggerFactory.getLogger(Compress.class);
	private static final String POSITION = "position";

	public static class MappClass extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String subStr = "";
			int pos = context.getConfiguration().getInt(POSITION, 0);

			// log.info("--- position: " + pos);

			if (line.contains("[")) {
				subStr = line.substring(line.indexOf("["));
				line = line.substring(0, line.indexOf("["));
			}
			if (pos == 0) { // 对于第一个字段组的情况
				context.write(new Text(""), new Text("(" + line + ")" + subStr));
				return;
			}
			int prepos = 0;
			int curpos = 0;
			for (int i = 0; i < pos; i++) {
				curpos = line.indexOf(",", prepos + 1);
				prepos = curpos;
			}
			String newKey = line.substring(0, prepos);
			String newValue = "(" + line.substring(prepos + 1) + ")";
			context.write(new Text(newKey), new Text(newValue + subStr));
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
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

	public static void setJobPath(Job job, String input, String output)
			throws Exception {
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
	}

	public static void initJob(Job job) throws Exception {

		job.setJarByClass(Compress.class);
		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NoSeperatorTextOutputFormate.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}

	public static void main(String[] args) throws Exception {
		//test();
		dataHandler();
	}

	public static void test() throws Exception {
		String input = "file:///home/ym/ytmp/data/test";
		String outputbase = "file:///home/ym/ytmp/data/output/test";

		String output = null;
		int[] positions = new int[] { 0,1, 2, 4 };

		Configuration conf = new Configuration();
		Job job = null;

		output = outputbase;
		for (int i = 0; i < positions.length; i++) {
			conf.set(POSITION, String.valueOf(positions[positions.length - i - 1]));
			job = new Job(conf);
			initJob(job);
			setJobPath(job, input, output);
			job.waitForCompletion(true);
			input = output;
			output = outputbase + i;
		}
	}
	
	public static void dataHandler() throws Exception{
		String input = "file:///home/ym/ytmp/data/1mRsample-H";
		String outputbase = "file:///home/ym/ytmp/data/output/1mRsample-H-r";
		String output = null;
		int[] positions = new int[] { 0, 2, 7, 18, 19, 21, 24 };

		Configuration conf = new Configuration();
		Job job = null;

		output = outputbase;
		for (int i = 0; i < positions.length; i++) {
			conf.set(POSITION, String.valueOf(positions[positions.length - i - 1]));
			job = new Job(conf);
			initJob(job);
			setJobPath(job, input, output);
			job.waitForCompletion(true);
			input = output;
			output = outputbase + i;
		}
	}
}