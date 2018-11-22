package edu.hebrew.db.external;

import java.io.*;
import java.sql.SQLOutput;
import java.util.*;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.BufferedReader;

public class ExternalMemoryImpl implements IExternalMemory {

	private static final int blocksNum = 1000;
	private static final int bytesInBlock = 20000;

	private String getField(String line, int colNum){
		return line.split(" ")[colNum - 1];
	}

	private void firstStage(BufferedReader reader, BufferedWriter writer, int colNum, int readChunks, long linesNum) throws Exception{

		double iterations = Math.ceil((double)linesNum / readChunks);
		for (int i = 0; i < iterations; i++) {
			Map<String, String> map = new TreeMap<String, String>();

			// Read chunk of lines from file to hash table
			for (int row = 0; row < readChunks; row++) {
				String line = "";
				if ((line = reader.readLine()) != null) {
					if (map.containsKey(getField(line, colNum))) {
						String key = getField(line, colNum);
						map.put(key, map.get(key) + System.lineSeparator() + line);
					}
					else{
						map.put(getField(line, colNum), line);
					}
				}
				else {
					break;
				}
			}

			// Write lines sorted by key to tmp
			for (String l : map.values()){
				writer.write(l);
				writer.newLine();
			}
		}
	}

	@Override
	public void sort(String in, String out, int colNum, String tmpPath) {
		try {
			File file = new File(in);
			BufferedReader reader = new BufferedReader(new FileReader(file));

			File fout = new File(tmpPath + "tmp.txt");
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));

			int lineLength = reader.readLine().length();
			int readChunks = (int) Math.ceil((bytesInBlock * blocksNum) / (lineLength * 2)); // lines can go into RAM


			// TODO: back to first line
			//reader.mark(0);
			//reader.reset();
			reader = new BufferedReader(new FileReader(file));
			firstStage(reader, writer, colNum, readChunks, (file.length() / lineLength));
			writer.close();
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