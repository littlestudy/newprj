package org.v1_2.statistics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.parser.JSONParser;

public class JsonToCsvTools {

	private static final JSONParser jsonParser = new JSONParser();
	private static List<String[]> targetFileds = getTargetFileds();
	
	public static void main(String[] args) {
		String input = "/home/ym/ytmp/data/statistics/1mRsample";
		String output = "/home/ym/ytmp/data/statistics/1mRsample-H";
		
		convert(input, output);
	}

	private static void convert(String input, String output) {
		BufferedReader reader = null;
		PrintStream ps = null;

		try {
			reader = new BufferedReader(new FileReader(input));
			ps = new PrintStream(output);

			String jsonString = null;
			String value = null;
			while ((jsonString = reader.readLine()) != null) {
				value = JsonToCsv2.jsonStringToCsv(jsonParser, jsonString, targetFileds);
				ps.println(value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (ps != null)
				ps.close();

			try {
				if (reader != null)
					reader.close();
			} catch (Exception e2) {
			}
		}
	}

	public static List<String[]> getTargetFileds() {

		List<String[]> list = new ArrayList<String[]>();
		String[] group1 = { "appKey", "appVersion", "dataType" };
		String[] group2 = { "city", "ip", "isp", "logCity", "logProvince" };
		String[] group3 = { "deviceCarrier", "deviceHashMac", "deviceIMEI",
				"deviceMacAddr", "deviceModel", "deviceNetwork", "deviceOs",
				"deviceOsVersion", "deviceResolution", "deviceUdid",
				"appChannel" };
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

		return list;
	}

}
