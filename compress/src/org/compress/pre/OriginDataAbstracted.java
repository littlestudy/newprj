package org.compress.pre;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class OriginDataAbstracted {
	public static void main(String[] args) {
		String originFile = "/home/ym/ytmp/data/0701";
		String savedFile = "/home/ym/ytmp/data/1mR";
		long account = 1000000;
		abstractdata(originFile, savedFile, account);
	}

	public static void abstractdata(String originFile, String savedFile,
			long account) {
		BufferedReader reader = null;
		PrintStream ps = null;
		try {
			reader = new BufferedReader(new FileReader(originFile));
			ps = new PrintStream(savedFile);
			String line = null;
			for (int i = 0; i < account; i++) {
				line = reader.readLine();
				if (line == null)
					break;
				ps.println(line);
			}
			ps.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (reader != null)
					reader.close();
				if (ps != null)
					ps.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}