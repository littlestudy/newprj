package org.uncompress;

import java.util.ArrayList;
import java.util.List;

public class Uncompress {

	public static List<String> UncompressUtil(String records){
		int curPos = 0;
		String field = null;
		List<String> result = new ArrayList<String>();
		List<String> fileds = new ArrayList<String>();
		
		while (curPos < records.length()){
			field = getField(records, curPos);
			curPos += field.length();
			fileds.add(field.substring(1, field.length() - 1));
			
			if (records.charAt(curPos) == '['){
				curPos++;
			} else if (records.charAt(curPos) == '('){
				result.add(buildObject(fileds));
				fileds.remove(fileds.size() - 1);
			} else {
				result.add(buildObject(fileds));
				fileds.remove(fileds.size() - 1);
				while (curPos < records.length() && records.charAt(curPos) == ']' ){
					fileds.remove(fileds.size() - 1);
					curPos++;
				}
			}
		}
		return result;
	}
	
	private static String getField(String records, int StartPos){
		int endPos = records.indexOf(")", StartPos);
		
		return records.substring(StartPos, endPos + 1);
	}
	
	private static String buildObject(List<String> fields){
		StringBuilder sb = new StringBuilder();
		for (String field : fields){
			sb.append("," + field);
		}
		return sb.toString().substring(1);
	}
}
