package util;

import java.io.InputStreamReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigLoader {
	private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);
	
	public static String loadJsonFileFromJarPackage(String jsonFile) {
		return loadJsonFile(new InputStreamReader(ConfigLoader.class.getResourceAsStream(jsonFile)));
	}

	public static String loadJsonFileFromLocalFileSystem(String jsonFile) {
		FileReader reader = null;
		try {
			reader = new FileReader(jsonFile);
		}catch(FileNotFoundException fnfe) {
			LOG.error(fnfe.toString());
			return null;
		}
			
		return loadJsonFile(reader);
	}

	private static String loadJsonFile(InputStreamReader reader) {
		final int BUFFER_SIZE = 256;
	
		StringBuilder jsonStr = new StringBuilder();
		try {
			char buffer[] = new char[BUFFER_SIZE];
			int bytesRead = -1;
			while((bytesRead = reader.read(buffer, 0, BUFFER_SIZE)) != -1)
				jsonStr.append(buffer, 0, bytesRead);
		}catch(IOException ioe) {
			LOG.error(ioe.toString());
			return null;
		}finally {
			try {
				reader.close();
			}catch(IOException ioe) {
				LOG.error(ioe.toString());
				return null;
			}
		}
		
		return jsonStr.toString();
	}
}
