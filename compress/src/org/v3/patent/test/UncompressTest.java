package org.v3.patent.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.List;

import org.v1_2.patent.UnCompress;

public class UncompressTest {

	public static void main(String[] args) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader("/home/ym/ytmp/data/statistics/1mRsample-H-C"));
		String line = null;
		PrintStream ps = new PrintStream("/home/ym/ytmp/data/completetest2");
		while ((line = reader.readLine()) != null){
			List<String> list = UnCompress.UncompressUtil(line);
			printStrings(list, ps);
		}
		reader.close();
		ps.close();
	}

	private static void printStrings(List<String> list, PrintStream ps) {
		for (int i = 0; i < list.size(); i++)
			ps.println(list.get(i));
	}
}
