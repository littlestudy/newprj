package org.v1.pre;

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
	private static String[] targetFileds = new String[] {
			"appKey", "appVersion", "dataType" // 0
			//  1		2				3

			, "city", "ip", "isp", "logCity", "logProvince" // 3
			//  4		5	  6      7          8
			
			, "deviceCarrier", "deviceHashMac", "deviceIMEI", "deviceMacAddr",
			//   9				10					11			12
			"deviceModel", "deviceNetwork", "deviceOs", "deviceOsVersion",
			//   13				14				15			16
			"deviceResolution", "deviceUdid", "appChannel" // 8
			//   17					18				19

			, "userName"  //19
			//  20
			, "occurTime", "persistedTime"  // 20
			//   21				22
			// , "sessionUuid"

			, "eventId", "costTime", "logSource"  // 22
			//  23			24			25
			
			, "sessionStep"  // 25
	// , "attributes" //:{"toVer":"1.5.1","fromVer":"1.4.0"}}
	};
	private static final JSONParser parser = new JSONParser();
	private static final int buffer_size = 1000;
	
	public static void main(String[] args) {
		String input = "/home/ym/ytmp/data/1mRsample";
		String output= "/home/ym/ytmp/data/1mRsample-H";
		
		jsonToCsv(input, output);
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
				if (field == null || "".equals(field)){
					field = "##\t";
					sb.append("," + field.toString());	
				}else {
					String fieldStr = field.toString();
					if (fieldStr.contains(","))
						fieldStr = replaceStr(fieldStr);
					sb.append("," + fieldStr);
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return sb.toString().substring(1);
	}
	
	private static String replaceStr(String line){
		String[] strs = line.split(",");
		StringBuilder sb = new StringBuilder();
		for (String str : strs)
			sb.append("|" + str.trim());
		return sb.substring(1);
	}
	
}
