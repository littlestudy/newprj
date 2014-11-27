package org.v3.patent.ioformat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.v1_2.patent.Utils.JsonToCsv;

public class JsonToCsvInputForamt extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new JsonToCsvRecordReader();
	}

	public static class JsonToCsvRecordReader extends
			RecordReader<LongWritable, Text> {
		private static final Logger LOG = LoggerFactory
				.getLogger(JsonToCsvRecordReader.class);

		public static List<String[]> groups;

		private LineRecordReader reader = new LineRecordReader();

		private final Text value_ = new Text();
		private final JSONParser jsonParser = new JSONParser();

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// LOG.info("\n\t" + "-----------" + targetFileds.get(0)[0]);
			// LOG.info("\n\t" + "-----------" + reader.nextKeyValue());
			// LOG.info("\n\t" + "-----------" +
			// reader.getCurrentValue().toString());
			while (reader.nextKeyValue()) {
				String value = JsonToCsv.jsonStringToCsv(jsonParser, reader
						.getCurrentValue().toString(), groups);
				if (value != null) {
					value_.set(value);
					return true;
				}
			}
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value_;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}



	}
	
	public static void setGroups(List<String[]> groups) {			
		System.out.println("-------------->>  set group");

		JsonToCsvInputForamt.JsonToCsvRecordReader.groups = groups;
	}
}