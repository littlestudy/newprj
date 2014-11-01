package org.datascience.statistics.fieldsort;

import java.io.IOException;
import java.util.Iterator;

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

public class FieldSort {
	public static class MappClass extends Mapper<LongWritable, Text, FieldNumKey, Text>{
		private FieldNumKey fieldNumKey = new FieldNumKey();
		private Text outputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String [] fields = line.split("\t"); 
			String field = fields[0];
			String fvalue = fields[1];
			int num = Integer.parseInt(fields[2]);
			fieldNumKey.set(field, num);
			outputValue.set(num + "\t" + fvalue);
			context.write(fieldNumKey, outputValue);
		}
	}
	
	public static class ReduceClass extends Reducer<FieldNumKey, Text, Text, Text>{
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		private Text seperator = new Text("---------------------");
		
		@Override
		protected void reduce(FieldNumKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			 Iterator<Text> iter = values.iterator();
			 int i = 0;
			 Text value = null;
			 while (iter.hasNext() && i < 10){
				 value = iter.next();
				 outputKey.set(key.getField());
				 outputValue.set(value.toString());
				 context.write(outputKey, outputValue);
				 i++;
			 }
			 context.write(seperator, seperator);
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		String input = "/home/ym/ytmp/data/output/1mRsample/mrs";		
		String output = "/home/ym/ytmp/data/output/1mRsample/mrssss";
		
		runJob(input, output);
	}
	
	private static void runJob(String input, String output) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(FieldNumKey.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(FieldNumKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(FieldPartitioner.class);
		job.setSortComparatorClass(FieldNumComparator.class);
		job.setGroupingComparatorClass(FieldNumGroup.class);

		Path outputPath = new Path(output);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
	}
}
