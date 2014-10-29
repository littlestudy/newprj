package org.compress.pre;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ReservoirSampler {

	private static int sampler_size = 100;
	private static int sampler_num = 100;
	private static int max_sampler_num = 10000;
	private static List<String[]> reservoir = new ArrayList<String[]>();

	public static void main(String[] args) {
		String input = "/home/ym/ytmp/data/1mR";		
		String output = "/home/ym/ytmp/data/1mRsample";
		sampling(input, output);
	}

	public static void sampling(String input, String output) {
		BufferedReader reader = null;
		Random rand = new Random();
		try {
			reader = new BufferedReader(new FileReader(input));
			for (int i = 0; i < sampler_num; i++) {
				reservoir.add(read(reader));
			}

			for (int i = sampler_num; i < max_sampler_num; i++) {
				int r = rand.nextInt(i);
				if (r < sampler_num) {
					reservoir.set(r, read(reader));
				} else {
					jump(reader);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		write(output);
	}

	private static String[] read(BufferedReader reader) {
		String[] sampler = new String[sampler_size];

		try {
			for (int i = 0; i < sampler_size; i++)
				sampler[i] = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return sampler;
	}

	private static void jump(BufferedReader reader) {
		try {
			for (int i = 0; i < sampler_size; i++)
				reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void write(String output) {
		PrintStream ps = null;
		try {
			ps = new PrintStream(output);
			for (int i = 0; i < reservoir.size(); i++){
				for (int j = 0; j < sampler_size; j++){
					ps.println(reservoir.get(i)[j]);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			ps.close();
		}
	}
}
