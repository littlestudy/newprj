package org.test;

import java.util.List;

import org.v1.uncompress.UnCompress;

public class UnCompressTest {
	public static void main(String[] args) {
				
		String str = "(pris,1.5.0)[(e,51,222.211.94.10,电信ADSL,成都市)[(四川,##	,##	,##	,##	,##	,##	,iPhone,##	,640/960,##	)[(##	,##	,1372654677000)[(##	,app_update,0)[(nosource,1)](##	,search,0)[(pris_app,2)]]]]](pris,2.2.0)[(e,37,117.136.32.16,移动(全省通用),##	)[(山东,##	,d41d8cd98f00b204e9800998ecf8427e,##	,##	,##	,##	,Android,4.1.2,480/800,##	)[(130221001,##	,1372638487000)[(##	,openmid,0)[(pris_app,1)]]]](e,45,121.31.247.158,联通,广西)[(广西,##	,d41d8cd98f00b204e9800998ecf8427e,##	,##	,##	,##	,Android,2.3.6,320/480,##	)[(130221001,##	,1372644764000)[(##	,openmid,0)[(pris_app,1)]]]]]";
		List<String> list = UnCompress.UncompressUtil(str);
		for (String s : list)
			System.out.println(s);
	}
}
