package org.v1_2.patent;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SplitRecordTreeMapReduceBygroup {

	public static class MapperClass extends
			Mapper<LongWritable, Text, Text, Text> {
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int curPos = 0;
			int depth = 0;
			String field = null;
			String records = value.toString();
			
			while (curPos < records.length()) {
				field = getField(records, curPos);
				depth++;
				curPos += field.length();
				outputKey.set(String.format("%1$02d", depth));
				outputValue.set(field.substring(1, field.length() - 1));
				context.write(outputKey, outputValue);
				
				if (records.charAt(curPos) == '[') {
					curPos++;
				} else if (records.charAt(curPos) == '(') {
					depth--;
				} else {
					depth--;
					while (curPos < records.length() && records.charAt(curPos) == ']') {
						depth--;
						curPos++;
					}
				}
			}			
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
		}
	}
	
	public static void main(String[] args) throws Exception {
		String input = "file:///home/ym/ytmp/data/output/newtestoo";
		String outputbase = "file:///home/ym/ytmp/data/output/newtestoo-";
		runJob(input, outputbase);
	}
	
	private static void runJob(String input, String output) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		
		job.setJarByClass(SplitRecordTreeMapReduceBygroup.class);
		job.setMapperClass(MapperClass.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);		
	}
	 
	private static String getField(String records, final int startPos) {
		int dep = 1;
		int searchPos = startPos + 1;
		while (dep > 0) {
			if (records.charAt(searchPos) == '(')
				dep++;
			else if (records.charAt(searchPos) == ')')
				dep--;
			searchPos++;
		}
		String field = records.substring(startPos, searchPos);
		
		return field;
	}
	
	
	/*
	public static List<String> UncompressUtil(String records) {
		int curPos = 0;
		String field = null;
		List<String> result = new ArrayList<String>();
		List<String> fileds = new ArrayList<String>();
		while (curPos < records.length()) {
			field = getField(records, curPos);
			curPos += field.length();
			fileds.add(field.substring(1, field.length() - 1));
			if (records.charAt(curPos) == '[') {
				curPos++;
			} else if (records.charAt(curPos) == '(') {
				result.add(buildObject(fileds));
				fileds.remove(fileds.size() - 1);
			} else {
				result.add(buildObject(fileds));
				fileds.remove(fileds.size() - 1);
				while (curPos < records.length()
						&& records.charAt(curPos) == ']') {
					fileds.remove(fileds.size() - 1);
					curPos++;
				}
			}
		}
		return result;
	}
	 
	private static String getField(String records, int startPos) {
		int dep = 1;
		int searchPos = startPos + 1;
		while (dep > 0) {
			if (records.charAt(searchPos) == '(')
				dep++;
			else if (records.charAt(searchPos) == ')')
				dep--;
			searchPos++;
		}

		return records.substring(startPos, searchPos);
	}

	private static String buildObject(List<String> fields) {
		StringBuilder sb = new StringBuilder();
		for (String field : fields) {
			sb.append("," + field);
		}
		return sb.toString().substring(1);
	}
	*/
}