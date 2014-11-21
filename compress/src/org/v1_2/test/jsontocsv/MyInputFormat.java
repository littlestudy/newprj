package org.v1_2.test.jsontocsv;

import java.util.ArrayList;
import java.util.List;

import org.v1_2.compress.JsonToCsvInputForamt;

public class MyInputFormat extends JsonToCsvInputForamt{
	/*
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
	*/
	
	public static List<String[]> getTargetFileds2() {
		System.out.println("--MyInputFormat");
		List<String[]> list = new ArrayList<String[]>();
		String[] group1 = {"appKey", "appVersion", "dataType"};
		String[] group2 = { "city", "ip", "isp", "logCity", "logProvince"};
		String[] group3 = {"deviceCarrier", "deviceHashMac", "deviceIMEI", "deviceMacAddr"
							, "deviceModel", "deviceNetwork", "deviceOs", "deviceOsVersion"
							, "deviceResolution", "deviceUdid", "appChannel"};
		String[] group4 = {"userName"};
		String[] group5 = {"occurTime", "persistedTime"};
		String[] group6 = {"eventId", "costTime", "logSource"};
		String[] group7 = {"sessionStep"};
		list.add(group1);
		list.add(group2);
		list.add(group3);
		list.add(group4);
		list.add(group5);
		list.add(group6);
		list.add(group7);
		
		JsonToCsvInputForamt.JsonToCsvRecordReader.groups = list;
		
		return list;
	}

}
