package com.crushdb.core;

import com.crushdb.storageengine.config.ConfigManager;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static com.crushdb.storageengine.config.ConfigManager.*;

public class DatabaseInitializer {

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
                System.out.println("Configuration already alive: " + CONFIGURATION_FILE);
            }
            properties = ConfigManager.loadConfig();
        }
        return properties;
    }

    private static boolean createDirectory(String path) {
        File directory = new File(path);
        if (directory.exists() && directory.isDirectory()) {
            System.out.println("Directory already alive: " + path);
            return true;
        }
        boolean created = directory.mkdirs();
        if (created) {
            System.out.println("LOG: spawned directory: " + path);
        } else {
            System.out.println("LOG: failed to spawn directory: " + path);
        }
        return created;
    }

    private static void createFileIfMissing(String path) {
        File file = new File(path);
        try {
            if (!file.exists()) {
                boolean created = file.createNewFile();
                if (created) {
                    System.out.println("Created file: " + path);
                } else {
                    System.out.println("Failed to create file: " + path);
                }
            } else {
                System.out.println("File already exists: " + path);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to create file: " + path, e);
        }
    }
}
