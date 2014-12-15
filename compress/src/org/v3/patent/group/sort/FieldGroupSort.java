package org.v3.patent.group.sort;

import java.io.IOException;
import java.math.BigDecimal;
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

public class FieldGroupSort {

	public static class MappClass extends
			Mapper<LongWritable, Text, FieldGroupSizeKey, Text> {
		private FieldGroupSizeKey fieldGroupSizeKey = new FieldGroupSizeKey();
		private Text outputValue = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			String field = fields[0];
			String fvalue = fields[1];
			int num = Integer.parseInt(fields[2]);

			long size = fvalue.getBytes().length * (num - 1);
			fieldGroupSizeKey.set(field, size);
			outputValue.set(String.valueOf(size) + "\t" + fvalue + "\t" + (num - 1));

			context.write(fieldGroupSizeKey, outputValue);

		}
	}

	public static class ReduceClass extends
			Reducer<FieldGroupSizeKey, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private Text seperator = new Text("---------------------");

		private static final int [] sizes = {376, 65636, 182215, 22214, 226174, 262771, 17126 };
		
		@Override
		protected void reduce(FieldGroupSizeKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			int i = 0;
			Text value = null;
			while (iter.hasNext() && i < 10) {
				value = iter.next();
				
				String field = key.getFieldGroup();
				String [] parts = value.toString().split("\t", -1);
				long repeatedSize = Long.parseLong(parts[0]);
				String fvalue = parts[1];
				int repeatedNum = Integer.parseInt(parts[2]);
				
				int num = Integer.parseInt(field) - 1;
				double	percent = (double) repeatedSize * 100 / sizes[num];	
				BigDecimal p = new BigDecimal(percent).setScale(2, BigDecimal.ROUND_HALF_UP);
				
				outputKey.set(key.getFieldGroup() );
				outputValue.set(repeatedSize + "\t" + fvalue + "\t" + repeatedNum + "\t" + p + "%");
				context.write(outputKey, outputValue);
				i++;
			}
			context.write(seperator, seperator);
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "/home/ym/ytmp/data/statistics/output/FieldGroupCount";
		String output = "/home/ym/ytmp/data/statistics/output/FieldGroupSort";

		runJob(input, output);
	}

	private static void runJob(String input, String output) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(FieldGroupSort.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(FieldGroupSizeKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(FieldGroupPartitioner.class);
		job.setSortComparatorClass(FieldGroupNumComparator.class);
		job.setGroupingComparatorClass(FieldGroupNumGroup.class);

		Path outputPath = new Path(output);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
	}
}
