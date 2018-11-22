package edu.hebrew.db.external;

import java.io.FileReader;
import java.io.BufferedReader;

public class ExternalMemoryImpl implements IExternalMemory {

	private static final int M = 1;
	private static final int Y = 200;
	private static final int RAM = 50000000;  // 50Mb in bytes
	private static final int COLSIZE = 20;

	private String getField(String line, int colNum){
		int from = (colNum - 1) * (COLSIZE + 1) + 1;
		if (colNum == 1) {
			from = 0;
		}
		int end = from + COLSIZE;
		return line.substring(from, end);
	}

	private void firstStage(BufferedReader reader, String in, String out, int colNum, String tmpPath, int readChunks){
		long iterations = Math.ceil(reader.length() / RAM)
		for (int i = 0; i < iterations; i++) {
			Map<String, String> map = new TreeMap<String, String>();
			for (int row = 0; row < readChunks; row++) {
				String line = "";
				line=reader.readLine()
				map.put(getField(line, colNum),line);

			}

		}


	}

	@Override
	public void sort(String in, String out, int colNum, String tmpPath) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(in));
			int lineLength = reader.readLine().length();
			int readChunks = Math.ceil(RAM / (lineLength * 2)); // lines can go into RAM
			reader.reset();
			firstStage(reader, in, out, colNum, tmpPath, readChunks);

		}
		catch(Exception e) {
			e.printStackTrace();
		}





		// TODO: Implement
	}

	@Override
	public void select(String in, String out, int colNumSelect,
			String substrSelect, String tmpPath) {
		// TODO: Implement
	}

	@Override
	public void sortAndSelectEfficiently(String in, String out, int colNumSort,
			String tmpPath, int colNumSelect, String substrSelect) {
		// TODO: Implement
	}

}