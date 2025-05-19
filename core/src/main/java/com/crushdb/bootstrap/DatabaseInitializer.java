package com.crushdb.bootstrap;

import com.crushdb.logger.CrushDBLogger;

import java.io.File;
import java.io.IOException;

import static com.crushdb.bootstrap.ConfigManager.*;

public class DatabaseInitializer {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(DatabaseInitializer.class);

    private DatabaseInitializer() {}

    public static CrushContext init() throws IllegalStateException {
        return createProdEnvironment();
    }

    public static TestCrushContext initTest() throws IllegalStateException {
        return createTestEnvironment();
    }

    private static CrushContext createProdEnvironment() {
        CrushContext cxt;
        boolean base = createDirectory(BASE_DIR);
        boolean log = createDirectory(LOG_DIR);
        boolean data = createDirectory(DATA_DIR);
        boolean crates = createDirectory(CRATES_DIR);
        boolean indexes = createDirectory(INDEXES_DIR);
        boolean wal = createDirectory(WAL_DIR);
        boolean certs = createDirectory(CU_CA_CERT_PATH);

        if (base && log && data && crates && indexes && wal && certs) {
            createFileIfMissing(DATABASE_FILE);
            createFileIfMissing(META_FILE);
            createFileIfMissing(JOURNAL_FILE);

            if (new File(CONFIGURATION_FILE).exists()) {
                LOGGER.info("Configuration already alive: " + CONFIGURATION_FILE, null);
            }
            cxt = ConfigManager.loadContext();
        } else {
            throw new IllegalStateException("Database directory structure failed to initialize.");
        }
        return cxt;
    }

    private static TestCrushContext createTestEnvironment() {
        TestCrushContext cxt;
        boolean base = createDirectory(TEST_BASE_DIR);
        boolean log = createDirectory(TEST_LOG_DIR);
        boolean data = createDirectory(TEST_DATA_DIR);
        boolean crates = createDirectory(TEST_CRATES_DIR);
        boolean indexes = createDirectory(TEST_INDEXES_DIR);
        boolean wal = createDirectory(TEST_WAL_DIR);
        boolean certs = createDirectory(TEST_CU_CA_CERT_PATH);

        if (base && log && data && crates && indexes && wal && certs) {
            createFileIfMissing(TEST_DATABASE_FILE);
            createFileIfMissing(TEST_META_FILE);
            createFileIfMissing(TEST_JOURNAL_FILE);

            if (new File(TEST_CONFIGURATION_FILE).exists()) {
                LOGGER.info("Test Configuration already alive: " + CONFIGURATION_FILE, null);
            }
            cxt = ConfigManager.loadTestContext();
        } else {
            throw new IllegalStateException("Database directory structure failed to initialize.");
        }
        return cxt;
    }

    private static boolean createDirectory(String path) {
        File directory = new File(path);
        if (directory.exists() && directory.isDirectory()) {
            LOGGER.info("Directory already alive: " + path, null);
            return true;
        }
        boolean created = directory.mkdirs();
        if (created) {
            LOGGER.info("LOG: spawned directory: " + path, null);
        } else {
            LOGGER.info("LOG: failed to spawn directory: " + path, null);
        }
        return created;
    }

    private static void createFileIfMissing(String path) {
        File file = new File(path);
        try {
            if (!file.exists()) {
                boolean created = file.createNewFile();
                if (created) {
                    LOGGER.info("Created file: " + path, null);
                } else {
                    LOGGER.info("Failed to create file: " + path, null);
                }
            } else {
                LOGGER.info("File already exists: " + path, null);
            }
        } catch (IOException e) {
            LOGGER.error("Unable to create file on path " + path + ": " + e, IllegalArgumentException.class.getName());
            throw new IllegalArgumentException(e);
        }
    }
}
