package com.crushdb.core;

import com.crushdb.storageengine.config.ConfigManager;

import java.io.File;

import static com.crushdb.storageengine.config.ConfigManager.*;

public class DatabaseInitializer {

    public static boolean init() {
        boolean isInit = false;
        boolean base = createDirectory(BASE_DIR);
        boolean log = createDirectory(LOG_DIR);
        boolean data = createDirectory(DATA_DIR);
        boolean wal = createDirectory(WAL_DIR);
        boolean certs = createDirectory(CU_CA_CERT_PATH);

        if (base && log && data && wal && certs) {
            if(!new File(CONFIGURATION_FILE).exists()) {
                isInit = ConfigManager.loadConfig();
            } else {
                System.out.println("Configuration already alive: " + CONFIGURATION_FILE);
                isInit = true;
            }
        }
        return isInit;
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
