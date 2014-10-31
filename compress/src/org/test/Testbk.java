package org.test;

import java.io.BufferedReader;
import java.io.FileReader;

public class Testbk {
	public static void main(String[] args) throws Exception {
		String input = "/home/ym/ytmp/data/1mRsample";
		test(input);
	}
	
	public static void test(String input) throws Exception{
		BufferedReader reader = new BufferedReader(new FileReader(input));
		
		String line = null;
		while ((line = reader.readLine()) != null){
			if (line.contains("["))
				System.out.println("*****");
		}
		
		reader.close();
	}
}
