package com.crushdb.storageengine.config;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Manages CrushDB's configuration by reading from `crushdb.conf`.
 * This class loads settings from the file into memory and provides access methods.
 *
 * @author Wali Morris
 */
public class ConfigManager {

    /**
     * Defines the base directory for CrushDB storage.
     * This directory contains all database-related files, including data, logs, and configurations.
     */
    public static final String BASE_DIR = Paths.get(System.getProperty("user.home")) + "/.crushdb/";

    /**
     * Path to the main configuration file for CrushDB.
     * This file stores database settings such as page size, WAL settings, and storage paths.
     */
    public static final String CONFIGURATION_FILE = BASE_DIR + "crushdb.conf";

    /**
     * Directory where general application logs (errors, debug, performance metrics) are stored.
     * This is separate from WAL logs to maintain clarity between operational and transactional logs.
     */
    public static final String LOG_DIR = BASE_DIR + "log/";

    /**
     * Directory where database data files (pages, indexes, etc.) are stored.
     * This includes all persistent storage related to the database.
     */
    public static final String DATA_DIR = BASE_DIR + "data/";

    /**
     * Directory where Write-Ahead Log (WAL) files are stored.
     * WAL files ensure durability and recovery in case of a crash.
     */
    public static final String WAL_DIR = BASE_DIR + "wal/";

    /**
     * Path to the directory where CrushDB stores custom user-supplied certificates.
     * This directory allows users to override the default system CA certificate if needed.
     * If a custom CA certificate is placed in this directory, CrushDB will use it for
     * TLS verification instead of the default system CA.
     * Default location: {@code ~/.crushdb/certs}
     */
    public static final String CU_CA_CERT_PATH = BASE_DIR + "certs/";

    private static final Properties properties = new Properties();

    public static boolean loadConfig() {
        Path configPath = Paths.get(CONFIGURATION_FILE);
        if (!Files.exists(configPath)) {
            writeDefaultConfig();
        }
        try (BufferedReader reader = Files.newBufferedReader(configPath)) {
            properties.load(reader);
            return true;
        } catch (IOException e) {
            System.err.println("Error loading CrushDB configuration: " + e.getMessage());
            return false;
        }
    }

    private static void writeDefaultConfig() {
        Path configPath = Paths.get(CONFIGURATION_FILE);
        try (InputStream inputStream = ConfigManager.class.getClassLoader().getResourceAsStream("config/crushdb.conf")) {
            if (inputStream == null) {
                throw new FileNotFoundException("Configuration file not found.");
            }
            Files.copy(inputStream, configPath, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("CrushDB Configuration file written: " + CONFIGURATION_FILE);
        } catch (IOException e) {
            System.err.println("Error writing CrushDB configuration: " + e.getMessage());
        }
    }

    public String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        return Integer.parseInt(properties.getProperty(key, String.valueOf(defaultValue)));
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(properties.getProperty(key, String.valueOf(defaultValue)));
    }
}

