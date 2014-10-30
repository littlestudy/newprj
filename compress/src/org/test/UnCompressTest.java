package org.test;

import java.util.List;

import org.compress.umcomp.UnCompress;

public class UnCompressTest {
	public static void main(String[] args) {
		String str = "(a)[(b)[(c,d)[(f,g)(h,#)](e,#)[(j,q)]](x)[(#,y)[(z,#)]]]";
		List<String> list = UnCompress.UncompressUtil(str);
		for (String s : list)
			System.out.println(s);
	}
}
