package com.crushdb.storageengine.config;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * StorageConfiguration handles the setup and initialization of CrushDB's storage system.
 * This class is responsible for:
 * <ul>
 *   <li>Creating necessary directories for storing data, logs, and Write-Ahead Log (WAL) files.</li>
 *   <li>Generating a configuration file (`crushdb.conf`) if it does not exist.</li>
 *   <li>Providing static constants for configuration settings, including page size and WAL behavior.</li>
 * </ul>
 *
 * The storage structure follows:
 * <pre>
 * ~/.crushdb/
 * ├── data/    # Stores database pages and indexes
 * ├── wal/     # Stores Write-Ahead Logs for crash recovery
 * ├── log/     # Stores error/debug logs
 * ├── crushdb.conf  # Configuration file
 * </pre>
 *
 * By default, CrushDB stores files in the user's home directory (`~/.crushdb/`),
 * but this can be configured via `crushdb.conf`.
 *
 * @author Wali Morris
 * @version 1.0
 */
public class StorageConfiguration {

    /**
     * Defines the base directory for CrushDB storage.
     * This directory contains all database-related files, including data, logs, and configurations.
     */
    private static final String BASE_DIR = Paths.get(System.getProperty("user.home")) + "/.crushdb/";

    /**
     * Path to the main configuration file for CrushDB.
     * This file stores database settings such as page size, WAL settings, and storage paths.
     */
    private static final String CONFIGURATION_FILE = BASE_DIR + "crushdb.conf";

    /**
     * Directory where general application logs (errors, debug, performance metrics) are stored.
     * This is separate from WAL logs to maintain clarity between operational and transactional logs.
     */
    private static final String LOG_DIR = BASE_DIR + "log/";

    /**
     * Directory where database data files (pages, indexes, etc.) are stored.
     * This includes all persistent storage related to the database.
     */
    private static final String DATA_DIR = BASE_DIR + "data/";

    /**
     * Directory where Write-Ahead Log (WAL) files are stored.
     * WAL files ensure durability and recovery in case of a crash.
     */
    private static final String WAL_DIR = BASE_DIR + "wal/";

    /**
     * Maximum size of a single page in CrushDB, defined in bytes.
     * Default is set to 4KB (0x1000).
     */
    public static final int MAX_PAGE_SIZE = 0x1000;

    /**
     * Tombstone grace period (in milliseconds) before deleted data is permanently removed.
     * This prevents immediate deletion to allow for recovery or conflict resolution.
     * Default is set to 60 seconds (60000 ms).
     */
    public static final long TOMBSTONE_GRACE_PERIOD_MS = 60 * 1000;

    /**
     * Flag indicating whether Write-Ahead Logging (WAL) is enabled.
     * If set to true, transactions are logged before being committed to disk.
     * This enhances durability and crash recovery.
     */
    public static final boolean WAL_ENABLED = true;

    /**
     * Configuration file field representing the storage path.
     * This field is used in the configuration file to specify where database files should be stored.
     * Example: storage_path=/home/user/.crushdb/data/
     */
    public static final String STORAGE_PATH_FIELD = "storage_path=";

    /**
     * Configuration file field representing the database page size.
     * This field defines the size of a single page in the crushdb storage engine.
     * Example: page_size=4096
     */
    public static final String PAGE_SIZE_FIELD = "page_size=";

    /**
     * Configuration file field indicating whether Write-Ahead Logging (WAL) is enabled.
     * If set to true, WAL is used for durability and crash recovery.
     * Example: wal_enabled=true
     */
    public static final String WAL_ENABLED_FIELD = "wal_enabled=";

    /**
     * Configuration file field defining the tombstone garbage collection grace period.
     * This determines how long deleted data (tombstones) should be retained before being permanently removed.
     * Example: tombstone_gc=60000 (milliseconds)
     */
    public static final String TOMBSTONE_GRACE_PERIOD_MS_FIELD = "tombstone_gc=";

    public static boolean init() {

        boolean isInit = false;
        boolean base = createDirectory(BASE_DIR);
        boolean log = createDirectory(LOG_DIR);
        boolean data = createDirectory(DATA_DIR);
        boolean wal = createDirectory(WAL_DIR);

        if (base && log && data && wal) {
            isInit = createConfiguration();
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

    private static boolean createConfiguration() {
        File config = new File(CONFIGURATION_FILE);
        if (config.exists() && config.isFile()) {
            System.out.println("Configuration file has already spawned: " + CONFIGURATION_FILE);
            return true;
        }
        try (FileWriter writer = new FileWriter(config)) {
            writer.write(String.format("%s%d\n", PAGE_SIZE_FIELD, MAX_PAGE_SIZE));
            writer.write(String.format("%s%b\n", WAL_ENABLED_FIELD, WAL_ENABLED));
            writer.write(String.format("%s%d\n", TOMBSTONE_GRACE_PERIOD_MS_FIELD, TOMBSTONE_GRACE_PERIOD_MS));
            writer.write(String.format("%s%s", STORAGE_PATH_FIELD, DATA_DIR));
            System.out.println("CrushDB Configuration file spawned: " + CONFIGURATION_FILE);
            return true;
        }  catch (IOException e) {
            System.err.println("Error spawning CrushDB configuration: " + e.getLocalizedMessage());
            return false;
        }
    }
}
