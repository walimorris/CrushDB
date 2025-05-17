package com.crushdb;

import com.crushdb.storageengine.config.ConfigManager;

import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

public class CrushContext extends Properties {
    private String baseDir;
    private boolean isTest;
    private boolean eagerLoadPages;
    private boolean autoCompressOnInsert;
    private boolean walEnabled;
    private boolean tlsEnabled;
    private String configPath;
    private String storagePath;
    private String dataPath;
    private String metaFilePath;
    private String cratesPath;
    private String indexesPath;
    private String logDirectory;
    private String walDirectory;
    private String walPath;
    private String caCertPath;
    private String customCaCertPath;
    private String logLevel;
    private int cacheMemoryLimitMb;
    private int cacheMaxPages;
    private int pageSize;
    private int tombstoneGc;
    private int logMaxFiles;
    private int logRetentionDays;
    private int logMaxSizeMb;

    public static final String BASE_DIR = "baseDir";
    public static final String IS_TEST = "isTest";
    public static final String CONFIG_PATH = "configPath";
    public static final String STORAGE_PATH = "storagePath";
    public static final String DATA_PATH = "dataPath";
    public static final String CRATES_PATH = "cratesPath";
    public static final String INDEXES_PATH = "indexesPath";
    public static final String META_FILE_PATH = "metaFilePath";
    public static final String CACHE_MEMORY_LIMIT_MB = "cacheMemoryLimitMb";
    public static final String CACHE_MAX_PAGES = "cacheMaxPages";
    public static final String PAGE_SIZE = "pageSize";
    public static final String EAGER_LOAD_PAGES = "eagerLoadPages";
    public static final String AUTO_COMPRESS_ON_INSERT = "autoCompressOnInsert";
    public static final String WAL_ENABLED = "walEnabled";
    public static final String TOMBSTONE_GC = "tombstoneGc";
    public static final String LOG_DIRECTORY = "logDirectory";
    public static final String LOG_MAX_FILES = "logMaxFiles";
    public static final String LOG_RETENTION_DAYS = "logRetentionDays";
    public static final String LOG_MAX_SIZE_MB = "logMaxSizeMb";
    public static final String LOGLEVEL = "logLevel";
    public static final String TLS_ENABLED = "tlsEnabled";
    public static final String CA_CERT_PATH = "caCertPath";
    public static final String CUSTOM_CA_CERT_PATH = "customCaCertPath";
    public static final String WAL_DIRECTORY = "walDirectory";
    public static final String WAL_PATH = "walPath";

    public CrushContext() {}

    public CrushContext(
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
        setProperty(BASE_DIR, baseDir);
        this.isTest = isTest;
        setProperty(IS_TEST, String.valueOf(isTest));
        this.eagerLoadPages = eagerLoadPages;
        setProperty(EAGER_LOAD_PAGES, String.valueOf(eagerLoadPages));
        this.autoCompressOnInsert = autoCompressOnInsert;
        setProperty(AUTO_COMPRESS_ON_INSERT, String.valueOf(autoCompressOnInsert));
        this.walEnabled = walEnabled;
        setProperty(WAL_ENABLED, String.valueOf(walEnabled));
        this.tlsEnabled = tlsEnabled;
        setProperty(TLS_ENABLED, String.valueOf(tlsEnabled));
        this.configPath = configPath;
        setProperty(CONFIG_PATH, configPath);
        this.storagePath = storagePath;
        setProperty(STORAGE_PATH, storagePath);
        this.dataPath = dataPath;
        setProperty(DATA_PATH, dataPath);
        this.metaFilePath = metaFilePath;
        setProperty(META_FILE_PATH, metaFilePath);
        this.cratesPath = cratesPath;
        setProperty(CRATES_PATH, cratesPath);
        this.indexesPath = indexesPath;
        setProperty(INDEXES_PATH, indexesPath);
        this.logDirectory = logDirectory;
        setProperty(LOG_DIRECTORY, logDirectory);
        this.walDirectory = walDirectory;
        setProperty(WAL_DIRECTORY, walDirectory);
        this.walPath = walPath;
        setProperty(WAL_PATH, walPath);
        this.caCertPath = caCertPath;
        setProperty(CA_CERT_PATH, caCertPath);
        this.customCaCertPath = customCaCertPath;
        setProperty(CUSTOM_CA_CERT_PATH, customCaCertPath);
        this.logLevel = logLevel;
        setProperty(LOGLEVEL, logLevel);
        this.cacheMemoryLimitMb = cacheMemoryLimitMb;
        setProperty(CACHE_MEMORY_LIMIT_MB, String.valueOf(cacheMemoryLimitMb));
        this.cacheMaxPages = cacheMaxPages;
        setProperty(CACHE_MAX_PAGES, String.valueOf(cacheMaxPages));
        this.pageSize = pageSize;
        setProperty(PAGE_SIZE, String.valueOf(pageSize));
        this.tombstoneGc = tombstoneGc;
        setProperty(TOMBSTONE_GC, String.valueOf(tombstoneGc));
        this.logMaxFiles = logMaxFiles;
        setProperty(LOG_MAX_FILES, String.valueOf(logMaxFiles));
        this.logRetentionDays = logRetentionDays;
        setProperty(LOG_RETENTION_DAYS, String.valueOf(logRetentionDays));
        this.logMaxSizeMb = logMaxSizeMb;
        setProperty(LOG_MAX_SIZE_MB, String.valueOf(logMaxSizeMb));
    }

    public void load(Reader reader, boolean isTest, String baseDir) throws IOException {
        load(reader);
        setProperty(IS_TEST, String.valueOf(isTest));
        setProperty(BASE_DIR, baseDir);
    }

    private void setFromReader() {
        if (getProperty(IS_TEST) != null && getProperty(BASE_DIR) != null) {
            if (Boolean.parseBoolean(getProperty(IS_TEST))) {
                baseDir = ConfigManager.TEST_BASE_DIR;
                setProperty(BASE_DIR, baseDir);
                logDirectory = ConfigManager.TEST_LOG_DIR;
                setProperty(LOG_DIRECTORY, logDirectory);
                dataPath = ConfigManager.TEST_DATA_DIR;
                setProperty(DATA_PATH, dataPath);
                cratesPath = ConfigManager.TEST_CRATES_DIR;
                setProperty(CRATES_PATH, cratesPath);
                indexesPath = ConfigManager.TEST_INDEXES_DIR;
                setProperty(INDEXES_PATH, indexesPath);
                walDirectory = ConfigManager.TEST_WAL_DIR;
                setProperty(WAL_DIRECTORY, walDirectory);
                walPath = ConfigManager.TEST_JOURNAL_FILE;
                setProperty(WAL_PATH, walPath);
                metaFilePath = ConfigManager.TEST_META_FILE;
                setProperty(META_FILE_PATH, metaFilePath);
                configPath = ConfigManager.TEST_CONFIGURATION_FILE;
                setProperty(CONFIG_PATH, configPath);
                storagePath = ConfigManager.TEST_DATABASE_FILE;
                setProperty(STORAGE_PATH, storagePath);
            }
            baseDir = getProperty(BASE_DIR);
            isTest = Boolean.parseBoolean(getProperty(IS_TEST));
            eagerLoadPages = Boolean.parseBoolean(getProperty(EAGER_LOAD_PAGES));
            autoCompressOnInsert = Boolean.parseBoolean(getProperty(AUTO_COMPRESS_ON_INSERT));
            walEnabled = Boolean.parseBoolean(getProperty(WAL_ENABLED));
            tlsEnabled = Boolean.parseBoolean(getProperty(TLS_ENABLED));
            configPath = getProperty(CONFIG_PATH);
            storagePath = getProperty(STORAGE_PATH);
            dataPath = getProperty(DATA_PATH);
            metaFilePath = getProperty(META_FILE_PATH);
            cratesPath = getProperty(CRATES_PATH);
            indexesPath = getProperty(INDEXES_PATH);
            logDirectory = getProperty(LOG_DIRECTORY);
            walDirectory = getProperty(LOG_DIRECTORY);
            walPath = getProperty(WAL_PATH);
            caCertPath = getProperty(CA_CERT_PATH);
            customCaCertPath = getProperty(CUSTOM_CA_CERT_PATH);
            logLevel = getProperty(LOGLEVEL);
            Integer.parseInt(properties.getProperty("cache_memory_limit_mb"));
            Integer.parseInt(properties.getProperty("cache_max_pages"));
            Integer.parseInt(properties.getProperty("page_size"));
            Integer.parseInt(properties.getProperty("tombstone_gc"));
            Integer.parseInt(properties.getProperty("log_max_files"));
            Integer.parseInt(properties.getProperty("log_retention_days"));
            Integer.parseInt(properties.getProperty("log_max_size_mb"));
        }
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        String val = getProperty(key);
        return (val == null || val.isEmpty()) ? defaultValue : val;
    }

    @Override
    public String getProperty(String key) {
        Object objectValue = get(key);
        String strValue = (objectValue instanceof String) ? (String) objectValue : null;
        Properties defaults = this.defaults;
        return ((strValue == null) && ((defaults) != null)) ? defaults.getProperty(key) : strValue;
    }
}
