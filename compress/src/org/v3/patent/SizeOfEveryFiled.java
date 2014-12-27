package org.v3.patent;

import java.io.IOException;
import java.math.BigDecimal;

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

public class SizeOfEveryFiled {
	public static class MappClass extends Mapper<LongWritable, Text, Text, LongWritable>{
		private Text mapOutputKey = new Text();
		private LongWritable mapOutputValue = new LongWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			/*
			//System.out.println(value.toString());
			String[] fields = value.toString().split(",", -1);
			for (int i = 0; i < fields.length; i++){
				mapOutputKey.set(String.format("%1$02d", i + 1));
				mapOutputValue.set(fields[i].getBytes().length);
				context.write(mapOutputKey, mapOutputValue);
			}
			*/
			String[] fields = value.toString().split("\t", -1);
			String field = fields[0];
			String fieldvalue = fields[1];
			int count = Integer.valueOf(fields[2]);
			for (int i = 0; i < count; i++){
				mapOutputKey.set(field);
				mapOutputValue.set(fieldvalue.getBytes().length);
				context.write(mapOutputKey, mapOutputValue);
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text, LongWritable, Text, Text>{
		private long totalCount = 0;
		private double totalPercent = 0.0;
		
		private Text outputValue = new Text();
		private final long clusterSize = 889164;
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable value : values)
				count += value.get();
			
			double percent = (double)count * 100 / clusterSize;
			
			totalPercent += percent;			
			totalCount += count;
			
			outputValue.set(String.valueOf(count) + "\t" 
					+ new BigDecimal(percent).setScale(2, BigDecimal.ROUND_HALF_UP) + "%");
			context.write(key, outputValue);
		}
		
		@Override
		protected void cleanup(
				Reducer<Text, LongWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("total count = " + totalCount);
			System.out.println("total percent = " + totalPercent);
		}
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/ym/ytmp/data/statistics/output/FieldCount333/part-r-00000";		
		String output = "/home/ym/ytmp/data/statistics/output/SizeOfEveryFiled222";
		
		runJob(input, output);
	}

	private static void runJob(String input, String output) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(SizeOfEveryFiled.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path outputPath = new Path(output);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
	}
}
