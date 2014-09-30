package cn.edu.tsinghua.util;

import java.io.InputStreamReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigLoader {
	private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);
	
	public static String loadJsonFileFromJarPackage(String jsonFile) throws IOException {
		return loadJsonFile(new InputStreamReader(ConfigLoader.class.getResourceAsStream(jsonFile)));
	}

    /*
     * Try to load configuration from file "jsonFile" in json in local filesystem.
     * If file "jsonFile" not found, load configuration from file "defaultJsonFile" in json packaged in jar.
     */
	public static String loadJsonFileFromLocalFileSystem(String jsonFile, String defaultJsonFile) throws IOException {
        try {
            return loadJsonFile(new FileReader(jsonFile));
        } catch (FileNotFoundException fnfe) {
            LOG.warn(fnfe.toString());

            return loadJsonFile(new InputStreamReader(ConfigLoader.class.getResourceAsStream(defaultJsonFile)));
        }
	}

	private static String loadJsonFile(InputStreamReader reader) throws IOException {
		final int BUFFER_SIZE = 256;
	
		StringBuilder jsonStr = new StringBuilder();
		try {
			char buffer[] = new char[BUFFER_SIZE];
			int bytesRead = -1;
			while((bytesRead = reader.read(buffer, 0, BUFFER_SIZE)) != -1)
				jsonStr.append(buffer, 0, bytesRead);
		}finally {
			try {
				reader.close();
			}catch(IOException ioe) {
				LOG.warn(ioe.toString());
			}
		}
		
		return jsonStr.toString();
	}
}