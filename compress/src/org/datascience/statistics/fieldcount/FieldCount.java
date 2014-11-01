package org.datascience.statistics.fieldcount;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FieldCount {

	public static class MappClass extends
			Mapper<LongWritable, Text, FieldValueKey, IntWritable> {
		private FieldValueKey fieldValueKey = new FieldValueKey();
		private IntWritable account = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",", -1);
			//if (fields.length == 27){
			//	for (String str : fields)
			//	System.out.println(str);
			//}
			for (int i = 0; i < fields.length; i++) {
				String field = String.format("%1$02d", i + 1);
				fieldValueKey.set(field, fields[i]);
				context.write(fieldValueKey, account);
			}
		}
	}

	public static class ReduceClass extends
			Reducer<FieldValueKey, IntWritable, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		//private Text seperator = new Text("---------------------");
		
		@Override
		protected void reduce(FieldValueKey key, Iterable<IntWritable> values,
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
		String input = "/home/ym/ytmp/data/1mRsample-H";		
		String output = "/home/ym/ytmp/data/output/1mRsample/mrs";
		
		runJob(input, output);
	}

	private static void runJob(String input, String output) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(FieldCount.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(FieldValueKey.class);
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
