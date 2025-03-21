package com.crushdb.core;

import com.crushdb.storageengine.config.ConfigManager;

import java.io.File;
import java.util.Properties;

import static com.crushdb.storageengine.config.ConfigManager.BASE_DIR;
import static com.crushdb.storageengine.config.ConfigManager.LOG_DIR;
import static com.crushdb.storageengine.config.ConfigManager.DATA_DIR;
import static com.crushdb.storageengine.config.ConfigManager.WAL_DIR;
import static com.crushdb.storageengine.config.ConfigManager.CU_CA_CERT_PATH;
import static com.crushdb.storageengine.config.ConfigManager.CONFIGURATION_FILE;

public class DatabaseInitializer {

    public static Properties init() {
        Properties properties = null;
        boolean base = createDirectory(BASE_DIR);
        boolean log = createDirectory(LOG_DIR);
        boolean data = createDirectory(DATA_DIR);
        boolean wal = createDirectory(WAL_DIR);
        boolean certs = createDirectory(CU_CA_CERT_PATH);

        if (base && log && data && wal && certs) {
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
}
