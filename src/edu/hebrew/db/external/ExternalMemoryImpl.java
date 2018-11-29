package edu.hebrew.db.external;

import java.io.*;
import java.util.*;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.BufferedReader;

public class ExternalMemoryImpl implements IExternalMemory {

	//** minHeap **//
	public class Entry implements Comparable<Entry> {
		private String line;
		private int index;

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


	//** class vars **//
	private PriorityQueue<Entry> minHeap = new PriorityQueue<>();

	private int readChunks = 0;
	private int lineInBytes = 0;
	private int colNum = 0;

	private ArrayList<Integer> tmpFiles = new ArrayList<>();
	private ArrayList<Integer> secondTmpFiles = new ArrayList<>();
	private ArrayList<BufferedReader> readers = new ArrayList<>();

	private static final int blocksNum = 1000;
	private static final int bytesInBlock = 20000;
	private int counter = 1;  // for new tmp files

	// for first stage in part C
	private boolean inPartC = false;
	private String subString;
	private int colWithSub;


	// ** CODE **//
	private String getField(String line, int colNum){
		return line.split(" ")[colNum - 1];
	}

	private void firstStage(BufferedReader reader, String tmpPath, int colNum, long linesNum) throws Exception{

		File fout;
		FileOutputStream fos;
		BufferedWriter writer;
		String col;

		int iterations = (int) Math.ceil((double)linesNum / this.readChunks);
		for (int i = 0; i < iterations; i++) {

			// create new file
			fout = new File(tmpPath + String.valueOf(counter) + ".txt");
			fos = new FileOutputStream(fout);
			writer = new BufferedWriter(new OutputStreamWriter(fos));

			Map<String, String> map = new TreeMap<String, String>();

			// Read chunk of lines from file to hash table
			for (int row = 0; row < this.readChunks; row++) {
				String line = "";
				if ((line = reader.readLine()) != null) {

					if (this.inPartC){
						col = getField(line, this.colWithSub);
						if (!col.contains(this.subString)){
							continue;
						}
					}

					if (map.containsKey(getField(line, colNum))) {
						String key = getField(line, colNum);
						map.put(key, map.get(key) + System.lineSeparator() + line);
					} else {
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
			writer.close();

		}
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


	private void secondStage(String tmpPath) throws Exception {

		File fout = new File(tmpPath + String.valueOf(counter) + ".txt");
		FileOutputStream fos = new FileOutputStream(fout);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));

		Entry min;
		makeReaders(tmpPath);
		String line;

		// iterate over all files and take min
		BufferedReader reader;

		int iteration = Math.min(this.readChunks, this.tmpFiles.size());

		// add first row to minHeap from every file we can fit to M
		for (int i = 0; i < iteration; i++) {
			reader = this.readers.get(i);

			if ((line = reader.readLine()) != null) {
				minHeap.add(new Entry(line, i));
			}
		}

		// while we still have lines in the chosen files
		while (	(min = minHeap.poll()) != null) {

			// write min line
			writer.write(min.line);
			writer.newLine();

			// advance reader with min value and get next line
			line = this.readers.get(min.index).readLine();

			if (line != null) {
				minHeap.add(new Entry(line, min.index));
			}
		}

		// delete iteration files
		for (int i = 0; i < iteration; i++) {
			File readFile;

			// override the prev file
			readFile = new File (tmpPath + String.valueOf(this.tmpFiles.get(0)) + ".txt");
			readFile.delete();

			// remove from tmpfiles
			this.tmpFiles.remove(0);
		}

		// add the merged file to tmp files
		this.secondTmpFiles.add(this.counter);
		writer.close();

		// if we iterate over all prev tmp files then update the new files as temps
		if (this.tmpFiles.size() <= 0){
			this.tmpFiles = this.secondTmpFiles;
			this.secondTmpFiles.clear();
		}
	}

	@Override
	public void sort(String in, String out, int colNum, String tmpPath) {
		// time checking
		//long startTime = System.nanoTime();

		try {
			File file = new File(in);
			BufferedReader reader = new BufferedReader(new FileReader(file));

			String tmps = tmpPath + "/tmp";
			this.colNum = colNum;

			this.lineInBytes = reader.readLine().length() * 2; // without \n

			// lines can go into M
			this.readChunks = (int) Math.floor((bytesInBlock * blocksNum) / (this.lineInBytes + 2));
			long linesNum = ((2*file.length() - lineInBytes) / (this.lineInBytes + 2)) + 1;

			reader = new BufferedReader(new FileReader(file));
			firstStage(reader, tmps, colNum, linesNum);

			if (this.tmpFiles.size() == 1){
				this.counter--;
			}
			// Loop until there is tmp files to merge
			while (this.tmpFiles.size() > 1 || this.secondTmpFiles.size() > 0) {
				secondStage(tmps);
			}
			new File(tmps + String.valueOf(counter) + ".txt").renameTo(new File(out));

			reader.close();
		}

		catch(Exception e) {
			e.printStackTrace();
		}

		// time checking
		//long endTime   = System.nanoTime();
		//long totalTime = endTime - startTime;
		//System.out.println(totalTime / 1000000000);
	}

	@Override
	public void select(String in, String out, int colNumSelect,
			String substrSelect, String tmpPath) {

		try {
			File file = new File(in);
			BufferedReader reader = new BufferedReader(new FileReader(file));

			File fout = new File(out);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));

			String line, col;
			while ((line = reader.readLine()) != null){
				col = getField(line, colNumSelect);
				if (col.contains(substrSelect)){
					writer.write(line);
					writer.newLine();
				}
			}

			writer.close();
			reader.close();
		}

		catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void sortAndSelectEfficiently(String in, String out, int colNumSort,
			String tmpPath, int colNumSelect, String substrSelect) {

		try {
			this.inPartC = true;
			this.subString = substrSelect;
			this.colWithSub = colNumSelect;

			sort(in, out, colNumSort, tmpPath);
		}

		catch(Exception e) {
			e.printStackTrace();
		}
	}

}