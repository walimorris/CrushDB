package com.crushdb.bootstrap;

import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.Properties;

public class CrushContext extends Properties {
    private String baseDir;
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

    protected CrushContext load(Reader reader, String baseDir) throws IOException {
        load(reader);
        setProperty(IS_TEST, String.valueOf(isTest()));
        setProperty(BASE_DIR, baseDir);
        setFromReader();

        // if the context is not meant for a test - ensure default '~'
        // is replaced with system parent directory
        if (!Boolean.parseBoolean(getProperty(IS_TEST))) {
            setSystemParent();
        }
        return this;
    }

    private void setSystemParent() {
        for (Map.Entry<Object, Object> prop: this.entrySet()) {
            String value = (String) prop.getValue();
            if (value.startsWith("~")) {
                value = value.replaceFirst("~", System.getProperty("user.home"));
            }
            setProperty((String) prop.getKey(), value);
        }
    }

    private void setFromReader() {
        if (getProperty(BASE_DIR) != null) {
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
            cacheMemoryLimitMb = Integer.parseInt(getProperty(CACHE_MEMORY_LIMIT_MB));
            cacheMaxPages = Integer.parseInt(getProperty(CACHE_MAX_PAGES));
            pageSize = Integer.parseInt(getProperty(PAGE_SIZE));
            tombstoneGc = Integer.parseInt(getProperty(TOMBSTONE_GC));
            logMaxFiles = Integer.parseInt(getProperty(LOG_MAX_FILES));
            logRetentionDays = Integer.parseInt(getProperty(LOG_RETENTION_DAYS));
            logMaxSizeMb = Integer.parseInt(getProperty(LOG_MAX_SIZE_MB));
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

    public String getBaseDir() {
        return baseDir;
    }

    public boolean isTest() {
        return false;
    }

    public boolean isEagerLoadPages() {
        return eagerLoadPages;
    }

    public boolean isAutoCompressOnInsert() {
        return autoCompressOnInsert;
    }

    public boolean isWalEnabled() {
        return walEnabled;
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    public String getConfigPath() {
        return configPath;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public String getDataPath() {
        return dataPath;
    }

    public String getMetaFilePath() {
        return metaFilePath;
    }

    public String getCratesPath() {
        return cratesPath;
    }

    public String getIndexesPath() {
        return indexesPath;
    }

    public String getLogDirectory() {
        return logDirectory;
    }

    public String getWalDirectory() {
        return walDirectory;
    }

    public String getWalPath() {
        return walPath;
    }

    public String getCaCertPath() {
        return caCertPath;
    }

    public String getCustomCaCertPath() {
        return customCaCertPath;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public int getCacheMemoryLimitMb() {
        return cacheMemoryLimitMb;
    }

    public int getCacheMaxPages() {
        return cacheMaxPages;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getTombstoneGc() {
        return tombstoneGc;
    }

    public int getLogMaxFiles() {
        return logMaxFiles;
    }

    public int getLogRetentionDays() {
        return logRetentionDays;
    }

    public int getLogMaxSizeMb() {
        return logMaxSizeMb;
    }
}
