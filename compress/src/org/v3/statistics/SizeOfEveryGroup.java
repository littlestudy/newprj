package org.v3.statistics;

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

public class SizeOfEveryGroup {
	public static class MappClass extends Mapper<LongWritable, Text, Text, LongWritable>{
		private Text mapOutputKey = new Text();
		private LongWritable mapOutputValue = new LongWritable();
		private long totalnum = 0;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//System.out.println(value.toString());
			String[] fields = value.toString().split("\t", -1);
			String groupFiled = fields[0];
			String groupFieldValue = fields[1];
			int num = Integer.parseInt(fields[2]);
			totalnum += num;
			long size = groupFieldValue.getBytes().length * num;
			mapOutputKey.set(groupFiled);
			mapOutputValue.set(size);
			context.write(mapOutputKey, mapOutputValue);
		}
		
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("totalnum -- > " + totalnum);
		}
	}
	
	public static class ReduceClass extends Reducer<Text, LongWritable, Text, Text>{
		private long totalCount = 0;
		private Text outputValue = new Text();
		private static final long totalSize = 2193343;
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable value : values)
				count += value.get();
			
			totalCount += count;
			
			double percent = (double)count * 100 / totalSize;
			
			outputValue.set(String.valueOf(count) + "\t" 
						+ new BigDecimal(percent).setScale(2, BigDecimal.ROUND_HALF_UP) + "%");
			
			context.write(key, outputValue);
		}
		
		@Override
		protected void cleanup(
				Reducer<Text, LongWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("total count = " + totalCount);			
		}
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/ym/ytmp/data/statistics/output/FieldGroupCount";		
		String output = "/home/ym/ytmp/data/statistics/output/SizeOfEveryGroup";
		
		runJob(input, output);
	}

	private static void runJob(String input, String output) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(SizeOfEveryGroup.class);

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
