package org.v1_2.compress;

import java.io.IOException;
import java.util.ArrayList;
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

		public static List<String[]> targetFileds;

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
						.getCurrentValue().toString(), targetFileds);
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

		public static List<String[]> getTargetFileds() {			
			System.out.println("-------------->>  getTargetFileds()");
			List<String[]> list = new ArrayList<String[]>();
			String[] group1 = { "appKey", "appVersion", "dataType" };
			String[] group2 = { "city", "ip", "isp", "logCity", "logProvince" };
			String[] group3 = { "deviceCarrier", "deviceHashMac", "deviceIMEI",
					"deviceMacAddr", "deviceModel", "deviceNetwork",
					"deviceOs", "deviceOsVersion", "deviceResolution",
					"deviceUdid", "appChannel" };
			String[] group4 = { "userName" };
			String[] group5 = { "occurTime", "persistedTime" };
			String[] group6 = { "eventId", "costTime", "logSource" };
			String[] group7 = { "sessionStep" };
			list.add(group1);
			list.add(group2);
			list.add(group3);
			list.add(group4);
			list.add(group5);
			list.add(group6);
			list.add(group7);

			JsonToCsvInputForamt.JsonToCsvRecordReader.targetFileds = list;

			return list;
			
		}

	}
	
	public static void getTargetFileds(){
		JsonToCsvRecordReader.getTargetFileds();
	}

}