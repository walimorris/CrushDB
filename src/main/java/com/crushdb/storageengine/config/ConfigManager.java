package com.crushdb.storageengine.config;

import com.crushdb.storageengine.page.PageManager;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStream;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * Manages CrushDB's configuration by reading from `crushdb.conf`.
 * This class loads settings from the file into memory and provides access methods.
 *
 * @author Wali Morris
 *
 * @see PageManager
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
     * Path to CrushDB configuration file. This file contains most database configuration properties.
     */
    public static final String DEFAULT_CONFIGURATION_FILE = "conf/crushdb.conf";

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
     * The file path for the data file used by CrushDB. This constant combines
     * the base data directory with the specific data file name "crushdb.db".
     * It defines the location of the primary data file where the persistent
     * data is stored.
     */
    public static final String DATABASE_FILE = DATA_DIR + "/crushdb.db";

    /**
     * Path to the directory where CrushDB stores custom user-supplied certificates.
     * This directory allows users to override the default system CA certificate if needed.
     * If a custom CA certificate is placed in this directory, CrushDB will use it for
     * TLS verification instead of the default system CA.
     * Default location: {@code ~/.crushdb/certs}
     */
    public static final String CU_CA_CERT_PATH = BASE_DIR + "certs/";

    /**
     * Max memory allowed to for caching pages - if there's a preference to utilize an explicit
     * page unit this property should be commented out and cache_maxPages will be used. It can
     * be noted that the page_size should be utilized for any calculations for best performance.
     */
    public static final String CACHE_MEMORY_LIMIT_MB_FIELD = "cache_memory_limit_mb";

    /**
     * Number of pages to keep in memory - if there's a preference to utilize memory unit size
     * this property should be commented out and cache_memoryLimitMB will be used. It can
     * be noted that the page_size should be utilized for any calculations for best performance.
     */
    public static final String CACHE_MAX_PAGES_FIELD = "cache_max_pages";

    /**
     * Configuration file field representing the database page size.
     * This field defines the size of a single page in the crushdb storage engine.
     * Example: page_size=4096
     */
    public static final String PAGE_SIZE_FIELD = "page_size";

    /**
     * Configuration file field defining the tombstone garbage collection grace period.
     * This determines how long deleted data (tombstones) should be retained before being permanently removed.
     * Example: tombstone_gc=60000 (milliseconds)
     */
    public static final String TOMBSTONE_GRACE_PERIOD_MS_FIELD = "tombstone_gc";

    /**
     * Configuration file field indicating whether Write-Ahead Logging (WAL) is enabled.
     * If set to true, WAL is used for durability and crash recovery.
     * Example: wal_enabled=true
     */
    public static final String WAL_ENABLED_FIELD = "wal_enabled";

    /**
     * Configuration file field indicating whether TLS (Transport Layer Security) is enabled.
     * When set to {@code true}, CrushDB enforces encrypted communication for secure data transmission.
     * Example configuration entry:{@code tls_enabled=true}
     */
    public static final String TLS_ENABLED_FIELD = "tls_enabled";

    /**
     * Configuration file field specifying the path to the system-wide CA (Certificate Authority) certificate file.
     * This certificate is used to verify the identity of external servers when establishing secure connections.
     * By default, CrushDB uses the standard CA certificate location for Debian/Ubuntu systems:
     * {@code /etc/ssl/certs/ca-certificates.crt}
     *
     * Example configuration entry:
     * {@code ca_cert_path=/etc/ssl/certs/ca-certificates.crt}
     */
    public static final String CA_CERT_PATH_FIELD = "ca_cert_path";

    /**
     * Configuration file field specifying the path to a custom user-supplied CA certificate file.
     * If set, CrushDB will use this certificate instead of the default system CA certificate.
     * This is useful for:
     * <ul>
     *   <li>Private cloud environments with custom certificate authorities.</li>
     *   <li>Enterprise security policies requiring non-standard CA certificates.</li>
     *   <li>Self-signed certificates for internal services.</li>
     * </ul>
     *
     * Example configuration entry:
     * {@code custom_ca_cert_path=~/.crushdb/certs/my_ca_cert.pem}
     */
    public static final String CU_CA_CERT_PATH_FIELD = "custom_ca_cert_path";

    /**
     * Configuration file field representing the storage path.
     * This field is used in the configuration file to specify where database files should be stored.
     * Example: storage_path=/home/user/.crushdb/data/
     */
    public static final String STORAGE_PATH_FIELD = "storage_path";

    /**
     * The maximum log size (in MB) before the system deletes old logs.
     * When the total log size in the `/log/` directory exceeds this limit,
     * the oldest log files are deleted to make space for new entries.
     */
    public static final String LOG_MAX_SIZE_MB_FIELD = "log_max_size_mb";

    /**
     * The number of days logs are retained before deletion. CrushDB handles log rotation
     * by periodically checking log files and enforcing a retention policy. This process
     * ensures that logs do not grow indefinitely, keeping the embedded database lightweight.
     */
    public static final String LOG_RETENTION_DAYS_FIELD = "log_retention_days";

    /**
     * Supported log levels for CrushDB.
     * CrushDB allows logging at different levels of severity:
     *     INFO  - General informational messages about database operations.
     *     ERROR - Critical errors or failures that require attention.
     */
    public static final String LOG_LEVEL = "log_level";

    public static final String LOG_MAX_FILES = "log_max_files";

    private static final Properties properties = new Properties();

    public static Properties loadConfig() {
        if (!Files.exists(Path.of(CONFIGURATION_FILE))) {
            writeDefaultConfig();
        }
        try (BufferedReader reader = Files.newBufferedReader(Path.of(CONFIGURATION_FILE))) {
            properties.load(reader);
            return properties;
        } catch (IOException e) {
            System.err.println("Error loading CrushDB configuration: " + e.getMessage());
            return null;
        }
    }

    private static void writeDefaultConfig() {
        try (InputStream inputStream = Files.newInputStream(Path.of(DEFAULT_CONFIGURATION_FILE))) {
            Files.copy(inputStream, Paths.get(CONFIGURATION_FILE), StandardCopyOption.REPLACE_EXISTING);
            System.out.println("CrushDB Configuration file written: " + CONFIGURATION_FILE);
        } catch (IOException e) {
            System.err.println("Error writing CrushDB configuration: " + e.getMessage());
        }
    }

    public static String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public static int getInt(String key, int defaultValue) {
        return Integer.parseInt(properties.getProperty(key, String.valueOf(defaultValue)));
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(properties.getProperty(key, String.valueOf(defaultValue)));
    }
}

