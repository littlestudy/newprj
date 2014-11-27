package org.v3.statistics;

import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonToCsv2 {	
	
	public static String jsonStringToCsv(JSONParser parser, String jsonString,
			List<String[]> targetFileds) {
		StringBuilder tsb = new StringBuilder();
		try {
			JSONObject jsonObj = (JSONObject) parser.parse(jsonString);
			for (int i = 0; i < targetFileds.size(); i++) {
				String[] fieldGroup = targetFileds.get(i);				
				for (int j = 0; j < fieldGroup.length; j++) {
					Object field = jsonObj.get(fieldGroup[j]);
					if (field == null || "".equals(field)) {
						field = "";
						tsb.append("," + field.toString());
					} else {
						String fieldStr = field.toString();
						if (fieldStr.contains(","))
							fieldStr = replaceStr(fieldStr);
						tsb.append("," + fieldStr);
					}
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return tsb.toString().substring(1);
	}

	private static String replaceStr(String line) {
		String[] strs = line.split(",");
		StringBuilder sb = new StringBuilder();
		for (String str : strs)
			sb.append("|" + str.trim());
		return sb.substring(1);
	}
}
