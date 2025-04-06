package com.crushdb.core;

import com.crushdb.logger.CrushDBLogger;
import com.crushdb.storageengine.config.ConfigManager;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static com.crushdb.storageengine.config.ConfigManager.*;

public class DatabaseInitializer {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(DatabaseInitializer.class);

    private DatabaseInitializer() {}

    public static Properties init() {
        Properties properties = null;
        boolean base = createDirectory(BASE_DIR);
        boolean log = createDirectory(LOG_DIR);
        boolean data = createDirectory(DATA_DIR);
        boolean wal = createDirectory(WAL_DIR);
        boolean certs = createDirectory(CU_CA_CERT_PATH);

        if (base && log && data && wal && certs) {
            createFileIfMissing(DATABASE_FILE);
            createFileIfMissing(META_FILE);

            if (new File(CONFIGURATION_FILE).exists()) {
                LOGGER.info("Configuration already alive: " + CONFIGURATION_FILE, null);
            }
            properties = ConfigManager.loadConfig();
        }
        return properties;
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
