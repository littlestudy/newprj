package org.compress.pre.testdata;

import java.io.FileNotFoundException;
import java.io.PrintStream;

public class CreateSequenceNum {

	public static void main(String[] args) {
		String output = "/home/ym/ytmp/rtest";
		create(output, 10000);
	}
	
	public static void create(String output, int num){
		PrintStream ps = null;
		try {
			ps = new PrintStream(output);
			for (int i = 0; i < num; i++)
				ps.println(i);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally{
			ps.close();
		}
	}

}
