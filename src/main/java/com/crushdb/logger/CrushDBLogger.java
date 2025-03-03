package com.crushdb.logger;

import com.crushdb.storageengine.config.ConfigManager;

import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

public class CrushDBLogger {
    private static int maxLogFiles;
    private static int maxLogRetentionDays;
    private static long maxLogSize;
    private static String logLevel;

    private static final String LOG_DIRECTORY = ConfigManager.LOG_DIR;
    private static final String LOG_FILE = LOG_DIRECTORY + "crushdb.log";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static {
        loadConfiguration();
    }

    private final String className;

    private CrushDBLogger(Class<?> clazz) {
        this.className = clazz.getSimpleName();
    }

    /**
     * Factory method to retrieve a logger instance for a given class.
     *
     * @param clazz The class requesting a logger.
     * @return A CrushDBLogger instance for the class.
     */
    public static CrushDBLogger getLogger(Class<?> clazz) {
        return new CrushDBLogger(clazz);
    }

    private static void loadConfiguration() {
        Properties properties = ConfigManager.loadConfig();
        if (properties != null) {
            maxLogFiles = parseInt(properties.getProperty(ConfigManager.LOG_MAX_FILES, "5"));
            maxLogRetentionDays = parseInt(properties.getProperty(ConfigManager.LOG_RETENTION_DAYS_FIELD, "7"));
            maxLogSize = parseLong(properties.getProperty(ConfigManager.LOG_MAX_SIZE_MB_FIELD, "50")) * 1024 * 1024;
            logLevel = properties.getProperty(ConfigManager.LOG_LEVEL, "INFO");
        } else {
            System.err.println("Error reading configuration. Using default log settings.");
        }
    }

    public int getMaxLogRetentionDays() {
        return maxLogRetentionDays;
    }

    public int getMaxLogFiles() {
        return maxLogFiles;
    }

    public long getMaxLogSize() {
        return maxLogSize;
    }

    public String getLogLevel() {
        return logLevel;
    }
}
