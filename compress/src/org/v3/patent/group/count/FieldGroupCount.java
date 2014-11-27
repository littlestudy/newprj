package org.v3.patent.group.count;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FieldGroupCount {

	public static class MappClass extends
			Mapper<LongWritable, Text, FieldGroupValueKey, IntWritable> {
		private FieldGroupValueKey fieldValueKey = new FieldGroupValueKey();
		private IntWritable account = new IntWritable(1);
		private long size = 0;
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fieldGroups = line.split("\t", -1);
			String fieldGroup = fieldGroups[0];
			String fieldGroupValue = fieldGroups[1];
			
			fieldValueKey.set(fieldGroup, fieldGroupValue);
			context.write(fieldValueKey, account);
		}
				
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, FieldGroupValueKey, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("-- size -- > " + size);
		}
	}

	public static class ReduceClass extends
			Reducer<FieldGroupValueKey, IntWritable, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		//private Text seperator = new Text("---------------------");
		
		@Override
		protected void reduce(FieldGroupValueKey key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			long account = 0;
			for (IntWritable i : values) {
				account += i.get();
			}
			outputKey.set(key.toString());
			outputValue.set(String.valueOf(account));
			context.write(outputKey, outputValue);
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			//context.write(seperator, seperator);			
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "/home/ym/ytmp/data/output/1mRsample-H-C/part-r-00000";		
		String output = "/home/ym/ytmp/data/output/1mRsample-H-C-group";
		
		runJob(input, output);
	}

	private static void runJob(String input, String output) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(FieldGroupCount.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);

		//JsonToCsvInputForamt.getTargetFileds();
		//job.setInputFormatClass(JsonToCsvInputForamt.class);

		job.setMapOutputKeyClass(FieldGroupValueKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(FieldPartitioner.class);
		job.setSortComparatorClass(FieldValueComparator.class);
		job.setGroupingComparatorClass(FieldValueComparator.class);

		Path outputPath = new Path(output);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
	}
}
