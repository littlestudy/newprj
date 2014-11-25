package org.v1_2.patent.test;

import java.util.List;

import org.v1_2.patent.UnCompress;

public class UncompressTest {

	public static void main(String[] args) {
		String str = "(a,b)[(c,d)[(e,f)]]";
		
		List<String> list = UnCompress.UncompressUtil(str);
		for (String s : list)
			System.out.println(s);
	}
}
