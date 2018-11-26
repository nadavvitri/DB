package edu.hebrew.db.external;

import java.io.*;
import java.util.*;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Comparator;

public class ExternalMemoryImpl implements IExternalMemory {

	public class Entry implements Comparable<Entry> {
		private String line;
		private int index;
		private int colNum;

		public Entry(String line, int index) {
			this.line = line;
			this.index = index;
		}

		private String getField(String line, int colNum){
			return line.split(" ")[colNum - 1];
		}

		@Override
		public int compareTo(Entry other) {
			return this.getField(this.line, colNum).compareTo(getField(other.line, colNum)) ;
		}
	}


	private PriorityQueue<Entry> minHeap = new PriorityQueue<>();

	private int[] sps;
	private int readChunks = 0;
	private int lineInBytes = 0;

	private ArrayList<Integer> tmpFiles = new ArrayList<>();
	private ArrayList<Integer> secondTmpFiles = new ArrayList<>();
	private ArrayList<BufferedReader> readers = new ArrayList<>();

	private static final int blocksNum = 1000;
	private static final int bytesInBlock = 20000;
	private int counter = 1;

	private String getField(String line, int colNum){
		return line.split(" ")[colNum - 1];
	}

	private void firstStage(BufferedReader reader, String tmpPath, int colNum, long linesNum) throws Exception{

		File fout = new File(tmpPath + String.valueOf(counter) + ".txt");
		FileOutputStream fos = new FileOutputStream(fout);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));

		int iterations = (int) Math.ceil((double)linesNum / this.readChunks);
		this.sps = new int[iterations];
		for (int i = 0; i < iterations; i++) {
			Map<String, String> map = new TreeMap<String, String>();

			// Read chunk of lines from file to hash table
			for (int row = 0; row < this.readChunks; row++) {
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

			// save new files
			this.tmpFiles.add(this.counter);

			// create new file
			this.counter++;
			fout = new File(tmpPath + String.valueOf(counter) + ".txt");
			fos = new FileOutputStream(fout);
			writer = new BufferedWriter(new OutputStreamWriter(fos));
		}

		writer.close();
	}

	private void makeReaders(String tmpPath) throws Exception{
		File file;
		BufferedReader reader;
		for (Integer num : this.tmpFiles) {
			file = new File(tmpPath + String.valueOf(num) + ".txt");
			reader = new BufferedReader(new FileReader(file));
			this.readers.add(reader);
		}
	}

	private String[] getMin(String[] rows, int colNum){
		String min = rows[0];
		Integer minIndex = 0;

		for (int i = 1; i < rows.length; i++) {
			// if cur <= line then the min = cur
			if (getField(rows[i], colNum).compareTo(getField(min, colNum)) <= 0) {
				min = rows[i];
				minIndex = i;
			}
		}

		return new String[]{min, String.valueOf(minIndex)};
	}


	private void secondStage(String tmpPath, BufferedWriter writer, int colNum) throws Exception {

		File fout = new File(tmpPath + String.valueOf(counter) + ".txt");
		FileOutputStream fos = new FileOutputStream(fout);
		writer = new BufferedWriter(new OutputStreamWriter(fos));

		Entry min;
		makeReaders(tmpPath);
		String newLine;

		// How many rows in M - 1 Blocks from each M - 1 files
		int read = (int) Math.floor(this.bytesInBlock / this.lineInBytes);

		// iterate over all files and take min
		BufferedReader reader;

		int iteration = Math.min(this.readChunks, this.tmpFiles.size());

		// add first row to minHeap from every file we can fit to M
		for (int i = 0; i < iteration; i++) {
			reader = this.readers.get(i);
			minHeap.add(new Entry(reader.readLine(), i));
		}

		// while we still have lines in the chosen files
		while (	(min = minHeap.poll()) != null) {

			// write min line
			writer.write(min.line);
			writer.newLine();

			// advance reader with min value and get next line
			newLine = this.readers.get(min.index).readLine();

			if (newLine != null) {
				minHeap.add(new Entry(newLine, min.index));
			}
		}

		// delete iteration files
		for (int i = 0; i < iteration; i++) {
			File readFile;

			// override the prev file
			readFile = new File (tmpPath + String.valueOf(this.tmpFiles.get(i)) + ".txt");
			readFile.delete();
		}

		// add the merged file to tmp files
		this.secondTmpFiles.add(this.counter);
	}

	@Override
	public void sort(String in, String out, int colNum, String tmpPath) {
		try {
			File file = new File(in);
			BufferedReader reader = new BufferedReader(new FileReader(file));

			String tmps = tmpPath + "tmp";

			this.lineInBytes = reader.readLine().length() * 2;
			this.readChunks = (int) Math.floor((bytesInBlock * blocksNum) / (this.lineInBytes)); // lines can go into M

			reader = new BufferedReader(new FileReader(file));
			firstStage(reader, tmps, colNum, (file.length() * 2) / this.lineInBytes);

			// Loop until there is tmp files to merge
			int iteration = this.tmpFiles.size();
			while ((this.tmpFiles.size() > 0) || (this.secondTmpFiles.size() > 0)){
				secondStage(tmpPath, );
			}
			//reader = new BufferedReader(new FileReader(fout));
			//secondStage(tmpPath, reader, writer, colNum);
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