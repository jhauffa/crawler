package edu.tum.cs.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class LogConfigurator {

	private static final Logger logger = Logger.getLogger(LogConfigurator.class.getName());
	private static final String defaultConfigFileName = "/logging.properties";

	static {
		InputStream is = null;
		try {
			String configFileName = System.getProperty("java.util.logging.config.file");
			if (configFileName != null)
				is = new FileInputStream(configFileName);
			else
				is = LogConfigurator.class.getResourceAsStream(defaultConfigFileName);
			if (is != null)
				LogManager.getLogManager().readConfiguration(is);
		} catch (IOException ex) {
			logger.log(Level.WARNING, "error reading logging configuration file", ex);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException ex) {
					logger.log(Level.WARNING, "error closing logging configuration file", ex);
				}
			}
		}
	}

}
