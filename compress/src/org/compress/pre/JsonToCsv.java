package org.compress.pre;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonToCsv {
	private static String[] targetFileds = new String[] { "appKey",
			"appVersion", "dataType"

			, "city", "ip", "isp", "logCity", "logProvince"

			, "deviceCarrier", "deviceHashMac", "deviceIMEI", "deviceMacAddr",
			"deviceModel", "deviceNetwork", "deviceOs", "deviceOsVersion",
			"deviceResolution", "deviceUdid", "appChannel"

			, "userName", "occurTime", "persistedTime"

			// , "sessionUuid"

			, "eventId", "costTime", "logSource"

			, "sessionStep"
	// , "attributes" //:{"toVer":"1.5.1","fromVer":"1.4.0"}}
	};
	private static final JSONParser parser = new JSONParser();
	private static final int buffer_size = 1000;
	
	public static void main(String[] args) {

	}

	public static void jsonToCsv(String input, String output) {
		BufferedReader reader = null;
		PrintStream ps = null;
		List<String> list = null;
		try {
			reader = new BufferedReader(new FileReader(input));			
			ps = new PrintStream(output);
			boolean isFinished = false;
			while (!isFinished){
				list =  new ArrayList<>(buffer_size);
				for (int i = 0; i < buffer_size; i++){
					String line = reader.readLine();
					if (line == null){
						isFinished = true;
						break;
					}
					list.add(jsonStringToCsv(line));
				}
				for (int i = 0; i < list.size(); i++){
					ps.println(list.get(i));
				}
				list = null;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			ps.close();
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static String jsonStringToCsv(String jsonString) {
		StringBuilder sb = new StringBuilder();
		try {
			JSONObject jsonObj = (JSONObject) parser.parse(jsonString);
			for (int i = 0; i < targetFileds.length; i++) {
				Object field = jsonObj.get(targetFileds[i]);
				if (field == null || "".equals(field))
					field = "##\t";
				sb.append("," + field.toString());
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return sb.toString().substring(1);
	}
}