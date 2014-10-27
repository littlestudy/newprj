package org.test;

import java.util.List;

import org.uncompress.Uncompress;

public class UncompressTest {

	public static void main(String[] args) {
		String line = "(a)[(b)[(c,d)[(f,g)(h,#)](e,#)[(j,q)]](x)[(#,y)[(z,#)]]]";
		List<String> list = Uncompress.UncompressUtil(line);
		for (String str : list)
			System.out.println(str);
	}
}
