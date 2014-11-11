package org.v1_2.datascience.statistics.fieldgroupcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.v1_2.compress.JsonToCsvInputForamt;

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
		String output = "/home/ym/ytmp/data/statistics/output/OutputOrigin";
		runJob(input, output);
	}
	
		private static void runJob(String input, String output) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(FieldGroupCount.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);

		JsonToCsvInputForamt.getTargetFileds();
		job.setInputFormatClass(JsonToCsvInputForamt.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path outputPath = new Path(output);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
	}
}
