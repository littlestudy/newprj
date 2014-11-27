package org.v3.statistics.field.size;

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
	public static class MappClass extends Mapper<LongWritable, Text, FieldSizeKey, Text>{
		private FieldSizeKey fieldSizeKey = new FieldSizeKey();
		private Text outputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String [] fields = line.split("\t"); 
			String field = fields[0];
			String fvalue = fields[1];
			int num = Integer.parseInt(fields[2]);
			
			long size = fvalue.getBytes().length * num;			
			fieldSizeKey.set(field, size);
			outputValue.set(fvalue + "\t" +num);
			
			context.write(fieldSizeKey, outputValue);
		}
	}
	
	public static class ReduceClass extends Reducer<FieldSizeKey, Text, Text, Text>{
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		private Text seperator = new Text("---------------------");
		
		@Override
		protected void reduce(FieldSizeKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			 Iterator<Text> iter = values.iterator();
			 int i = 0;
			 Text value = null;
			 while (iter.hasNext() && i < 10){
				 value = iter.next();
				 outputKey.set(key.getField() + "\t" + key.getSize());
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
		String input = "/home/ym/ytmp/data/statistics/output/FieldCount";		
		String output = "/home/ym/ytmp/data/statistics/output/FieldSort";
		
		runJob(input, output);
	}
	
	private static void runJob(String input, String output) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(FieldSizeKey.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(FieldSizeKey.class);
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
