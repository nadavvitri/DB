package edu.hebrew.db.external;

import java.io.FileReader;
import java.io.BufferedReader;

public class ExternalMemoryImpl implements IExternalMemory {

	private static final int M = 1;
	private static final int Y = 200;

	private void firstStage(String in, String out, int colNum, String tmpPath, int readChunks){

	}

	@Override
	public void sort(String in, String out, int colNum, String tmpPath) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(in));
			//int lineLength = reader.readLine().length();
			//int readChunks = (lineLength / Y) * M;
			//reader.reset();

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