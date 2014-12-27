package org.v1_2.datascience.statistics.fieldsizesort;

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

public class FieldSort2 {
	public static class MappClass extends
			Mapper<LongWritable, Text, FieldSizeKey, Text> {
		private FieldSizeKey fieldSizeKey = new FieldSizeKey();
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
			fieldSizeKey.set(field, size);
			outputValue.set(fvalue + "\t" + (num - 1));

			context.write(fieldSizeKey, outputValue);
		}
	}

	public static class ReduceClass extends
			Reducer<FieldSizeKey, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		private int[] fieldSize = { 112, 180, 28, 3061, 20734, 15896,
				10218, 9483, 0, 49984, 21101, 9401, 13356, 0, 14063, 9903,
				13433, 27342, 3782, 22214, 111709, 105872, 115124, 33377,
				96406, 17126 };

		private Text seperator = new Text(				
				"--------------------------------------------------------------------------------------------------"
				+ "---------------------------------------------------------------------------------------" + "\n" 
				+ "字段" + "\t\t\t" + "值" + "\t\t\t\t" + "重复次数" + "\t\t" + "重复量" + "\t\t\t" + "重复率");

		@Override
		protected void reduce(FieldSizeKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			context.write(seperator, new Text(""));
			
			
			Iterator<Text> iter = values.iterator();
			int i = 0;
			Text value = null;
			String field = null;
			String fvalue = null;
			int repeatedNum = 0;
			int repeatedSize = 0;
			double percent = 0.0;
			
			
			
			while (iter.hasNext() && i < 10) {
				value = iter.next();
				
				field = key.getField();
				fvalue = value.toString().split("\t", -1)[0];
				repeatedNum = Integer.parseInt(value.toString().split("\t", -1)[1]);
				repeatedSize = fvalue.getBytes().length * repeatedNum;
			
				int num = Integer.parseInt(field) - 1;
				
				int size = fieldSize[num];
				if (size == 0)
					percent = 0;
				else
					percent = (double) repeatedSize * 100 / fieldSize[num];
				
				BigDecimal p = new BigDecimal(percent).setScale(2, BigDecimal.ROUND_HALF_UP);
				
				outputKey.set(field + "\t\t\t" + fvalue + "\t\t\t\t" + repeatedNum + "\t\t\t" + repeatedSize);
				outputValue.set(String.valueOf(p.doubleValue()) + "%");
				context.write(outputKey, outputValue);
				i++;
			}
			

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "/home/ym/ytmp/data/statistics/output/FieldCount333/part-r-00000";
		String output = "/home/ym/ytmp/data/statistics/output/FieldSort222";

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
