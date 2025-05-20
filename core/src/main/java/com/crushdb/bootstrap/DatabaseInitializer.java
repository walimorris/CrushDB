package com.crushdb.bootstrap;

import com.crushdb.logger.CrushDBLogger;

import java.io.File;
import java.io.IOException;

import static com.crushdb.bootstrap.ConfigManager.*;

public class DatabaseInitializer {

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

            cxt = ConfigManager.loadContext();
            CrushDBLogger.loadConfiguration(cxt);
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

            cxt = ConfigManager.loadTestContext();
            CrushDBLogger.loadConfiguration(cxt);
        } else {
            throw new IllegalStateException("Database directory structure failed to initialize.");
        }
        return cxt;
    }

    private static boolean createDirectory(String path) {
        File directory = new File(path);
        if (directory.exists() && directory.isDirectory()) {
            return true;
        }
        return directory.mkdirs();
    }

    private static void createFileIfMissing(String path) {
        File file = new File(path);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
