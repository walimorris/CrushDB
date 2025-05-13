package com.crushdb;

import com.crushdb.storageengine.config.ConfigManager;

import java.util.Properties;

public class CrushContext {
    private final String baseDir;
    private final boolean isTest;
    private final boolean eagerLoadPages;
    private final boolean autoCompressOnInsert;
    private final boolean walEnabled;
    private final boolean tlsEnabled;
    private final String configPath;
    private final String storagePath;
    private final String dataPath;
    private final String metaFilePath;
    private final String cratesPath;
    private final String indexesPath;
    private final String logDirectory;
    private final String walDirectory;
    private final String walPath;
    private final String caCertPath;
    private final String customCaCertPath;
    private final String logLevel;
    private final int cacheMemoryLimitMb;
    private final int cacheMaxPages;
    private final int pageSize;
    private final int tombstoneGc;
    private final int logMaxFiles;
    private final int logRetentionDays;
    private final int logMaxSizeMb;

    private CrushContext(
            String baseDir,
            boolean isTest,
            boolean eagerLoadPages,
            boolean autoCompressOnInsert,
            boolean walEnabled,
            boolean tlsEnabled,
            String configPath,
            String storagePath,
            String dataPath,
            String metaFilePath,
            String cratesPath,
            String indexesPath,
            String logDirectory,
            String walDirectory,
            String walPath,
            String caCertPath,
            String customCaCertPath,
            String logLevel,
            int cacheMemoryLimitMb,
            int cacheMaxPages,
            int pageSize,
            int tombstoneGc,
            int logMaxFiles,
            int logRetentionDays,
            int logMaxSizeMb) {

        this.baseDir = baseDir;
        this.isTest = isTest;
        this.eagerLoadPages = eagerLoadPages;
        this.autoCompressOnInsert = autoCompressOnInsert;
        this.walEnabled = walEnabled;
        this.tlsEnabled = tlsEnabled;
        this.configPath = configPath;
        this.storagePath = storagePath;
        this.dataPath = dataPath;
        this.metaFilePath = metaFilePath;
        this.cratesPath = cratesPath;
        this.indexesPath = indexesPath;
        this.logDirectory = logDirectory;
        this.walDirectory = walDirectory;
        this.walPath = walPath;
        this.caCertPath = caCertPath;
        this.customCaCertPath = customCaCertPath;
        this.logLevel = logLevel;
        this.cacheMemoryLimitMb = cacheMemoryLimitMb;
        this.cacheMaxPages = cacheMaxPages;
        this.pageSize = pageSize;
        this.tombstoneGc = tombstoneGc;
        this.logMaxFiles = logMaxFiles;
        this.logRetentionDays = logRetentionDays;
        this.logMaxSizeMb = logMaxSizeMb;

    }

    public static CrushContext fromProperties(Properties properties) {
        String baseDirectory;
        String logDirectory;
        String dataPath;
        String cratesDirectory;
        String indexesDirectory;
        String walDirectory;
        String walPath;
        String metaFile;
        String configFile;
        String storagePath;

        if (!Boolean.parseBoolean(properties.getProperty("isTest"))) {
            baseDirectory = ConfigManager.BASE_DIR;
            logDirectory = ConfigManager.LOG_DIR;
            dataPath = ConfigManager.DATA_DIR;
            cratesDirectory = ConfigManager.CRATES_DIR;
            indexesDirectory = ConfigManager.INDEXES_DIR;
            walDirectory = ConfigManager.WAL_DIR;
            walPath = ConfigManager.JOURNAL_FILE;
            metaFile = ConfigManager.META_FILE;
            configFile = ConfigManager.CONFIGURATION_FILE;
            storagePath = ConfigManager.DATABASE_FILE;
        } else {
            baseDirectory = ConfigManager.TEST_BASE_DIR;
            logDirectory = ConfigManager.TEST_LOG_DIR;
            dataPath = ConfigManager.TEST_DATA_DIR;
            cratesDirectory = ConfigManager.TEST_CRATES_DIR;
            indexesDirectory = ConfigManager.TEST_INDEXES_DIR;
            walDirectory = ConfigManager.TEST_WAL_DIR;
            walPath = ConfigManager.JOURNAL_FILE;
            metaFile = ConfigManager.TEST_META_FILE;
            configFile = ConfigManager.TEST_CONFIGURATION_FILE;
            storagePath = ConfigManager.TEST_DATABASE_FILE;
        }

        return new CrushContext(
                baseDirectory,
                Boolean.parseBoolean(properties.getProperty("isTest")),
                Boolean.parseBoolean(properties.getProperty("eager_load_pages", "true")),
                Boolean.parseBoolean(properties.getProperty("autoCompressOnInsert", "false")),
                Boolean.parseBoolean(properties.getProperty("wal_enabled", "true")),
                Boolean.parseBoolean(properties.getProperty("tls_enabled", "false")),
                configFile,
                storagePath,
                dataPath,
                metaFile,
                cratesDirectory,
                indexesDirectory,
                logDirectory,
                walDirectory,
                walPath,
                properties.getProperty("ca_cert_path", ""),
                properties.getProperty("custom_ca_cert_path", ""),
                properties.getProperty("log_level", "INFO,ERROR"),
                Integer.parseInt(properties.getProperty("cache_memory_limit_mb")),
                Integer.parseInt(properties.getProperty("cache_max_pages")),
                Integer.parseInt(properties.getProperty("page_size")),
                Integer.parseInt(properties.getProperty("tombstone_gc")),
                Integer.parseInt(properties.getProperty("log_max_files")),
                Integer.parseInt(properties.getProperty("log_retention_days")),
                Integer.parseInt(properties.getProperty("log_max_size_mb"))
        );
    }

    public Properties toProperties() {
        Properties properties = new Properties();
        properties.setProperty("baseDir", baseDir);
        properties.setProperty("isTest", String.valueOf(isTest));
        properties.setProperty("eagerLoadPages", String.valueOf(eagerLoadPages));
        properties.setProperty("autoCompressOnInsert", String.valueOf(autoCompressOnInsert));
        properties.setProperty("walEnabled", String.valueOf(walEnabled));
        properties.setProperty("tlsEnabled", String.valueOf(tlsEnabled));
        properties.setProperty("configPath", configPath);
        properties.setProperty("storagePath", storagePath);
        properties.setProperty("dataPath", dataPath);
        properties.setProperty("metaFilePath", metaFilePath);
        properties.setProperty("cratesPath", cratesPath);
        properties.setProperty("indexesPath", indexesPath);
        properties.setProperty("logDirectory", logDirectory);
        properties.setProperty("walDirectory", walDirectory);
        properties.setProperty("walPath", walPath);
        properties.setProperty("caCertPath", caCertPath);
        properties.setProperty("customCaCertPath", customCaCertPath);
        properties.setProperty("logLevel", logLevel);
        properties.setProperty("cacheMemoryLimitMb", String.valueOf(cacheMemoryLimitMb));
        properties.setProperty("cacheMaxPages", String.valueOf(cacheMaxPages));
        properties.setProperty("pageSize", String.valueOf(pageSize));
        properties.setProperty("tombstoneGc", String.valueOf(tombstoneGc));
        properties.setProperty("logMaxFiles", String.valueOf(logMaxFiles));
        properties.setProperty("logRetentionDays", String.valueOf(logRetentionDays));
        properties.setProperty("logMaxSizeMb", String.valueOf(logMaxSizeMb));
        return properties;
    }
}
