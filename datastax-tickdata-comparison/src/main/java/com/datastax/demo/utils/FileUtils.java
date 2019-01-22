package com.datastax.demo.utils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FileUtils {

	public static List<String> readFileIntoList(String filename) {

		List<String> fileList = new ArrayList<String>();
		BufferedReader br = null;
		File file = new File("src/main/resources", filename);

		try {
			String currentLine;
			br = new BufferedReader(new FileReader(file));

			while ((currentLine = br.readLine()) != null) {
				fileList.add(currentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return fileList;
	}

	public static String readFileIntoString(String filename) {

		StringBuffer buffer = new StringBuffer();
		BufferedReader br = null;
		File file = new File("src/main/resources", filename);

		try {
			String currentLine;
			br = new BufferedReader(new FileReader(file));

			while ((currentLine = br.readLine()) != null) {
				buffer.append(currentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return buffer.toString();
	}

	public static long getObjectSize(Serializable ser) {

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(ser);
			
			oos.close();
			return baos.size();
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		return 0;
	}
}
