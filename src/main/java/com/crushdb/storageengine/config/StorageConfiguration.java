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
 * ├── certs/   # Stores custom user supplied certificates
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
     * Configuration file field representing the database page size.
     * This field defines the size of a single page in the crushdb storage engine.
     * Example: page_size=4096
     */
    public static final String PAGE_SIZE_FIELD = "page_size=";

    /**
     * Tombstone grace period (in milliseconds) before deleted data is permanently removed.
     * This prevents immediate deletion to allow for recovery or conflict resolution.
     * Default is set to 60 seconds (60000 ms).
     */
    public static final long TOMBSTONE_GRACE_PERIOD_MS = 60 * 1000;

    /**
     * Configuration file field defining the tombstone garbage collection grace period.
     * This determines how long deleted data (tombstones) should be retained before being permanently removed.
     * Example: tombstone_gc=60000 (milliseconds)
     */
    public static final String TOMBSTONE_GRACE_PERIOD_MS_FIELD = "tombstone_gc=";

    /**
     * Flag indicating whether Write-Ahead Logging (WAL) is enabled.
     * If set to true, transactions are logged before being committed to disk.
     * This enhances durability and crash recovery.
     */
    public static final boolean WAL_ENABLED = true;

    /**
     * Configuration file field indicating whether Write-Ahead Logging (WAL) is enabled.
     * If set to true, WAL is used for durability and crash recovery.
     * Example: wal_enabled=true
     */
    public static final String WAL_ENABLED_FIELD = "wal_enabled=";

    /**
     * Flag to enable or disable TLS (Transport Layer Security) for secure communication.
     * When set to {@code true}, CrushDB will enforce encrypted connections using TLS.
     * Default: {@code true}
     */
    private static final boolean TLS_ENABLED = true;

    /**
     * Configuration file field indicating whether TLS (Transport Layer Security) is enabled.
     * When set to {@code true}, CrushDB enforces encrypted communication for secure data transmission.
     * Example configuration entry:{@code tls_enabled=true}
     */
    public static final String TLS_ENABLED_FIELD = "tls_enabled=";

    /**
     * Default system CA (Certificate Authority) certificate path used for TLS verification.
     * This is the standard location on Debian/Ubuntu-based Linux systems where trusted CA
     * certificates are stored for validating SSL/TLS connections.
     * <p>
     *     Example:
     *     <ul>
     *         <li>Debian/Ubuntu: {@code /etc/ssl/certs/ca-certificates.crt}</li>
     *         <li>RHEL/CentOS: {@code /etc/pki/tls/certs/ca-bundle.crt}</li>
     *         <li>Alpine Linux: {@code /etc/ssl/cert.pem}</li>
     *     </ul>
     * </p>
     *
     * If CrushDB is running on a different Linux distribution, this path should be updated
     * accordingly in the configuration file.
     * Default: {@code /etc/ssl/certs/ca-certificates.crt}
     */
    private static final String CA_CERT_PATH = "/etc/ssl/certs/ca-certificates.crt";

    /**
     * Configuration file field specifying the path to the system-wide CA (Certificate Authority) certificate file.
     * This certificate is used to verify the identity of external servers when establishing secure connections.
     * By default, CrushDB uses the standard CA certificate location for Debian/Ubuntu systems:
     * {@code /etc/ssl/certs/ca-certificates.crt}
     *
     * Example configuration entry:
     * {@code ca_cert_path=/etc/ssl/certs/ca-certificates.crt}
     */
    public static final String CA_CERT_PATH_FIELD = "ca_cert_path=";

    /**
     * Path to the directory where CrushDB stores custom user-supplied certificates.
     * This directory allows users to override the default system CA certificate if needed.
     * If a custom CA certificate is placed in this directory, CrushDB will use it for
     * TLS verification instead of the default system CA.
     * Default location: {@code ~/.crushdb/certs}
     */
    private static final String CU_CA_CERT_PATH = BASE_DIR + "certs/";

    /**
     * Configuration file field specifying the path to a custom user-supplied CA certificate file.
     * If set, CrushDB will use this certificate instead of the default system CA certificate.
     * This is useful for:
     * <ul>
     *   <li>Private cloud environments with custom certificate authorities.</li>
     *   <li>Enterprise security policies requiring non-standard CA certificates.</li>
     *   <li>Self-signed certificates for internal services.</li>
     * </ul>
     *
     * Example configuration entry:
     * {@code custom_ca_cert_path=~/.crushdb/certs/my_ca_cert.pem}
     */
    public static final String CU_CA_CERT_PATH_FIELD = "custom_ca_cert_path=";

    /**
     * Configuration file field representing the storage path.
     * This field is used in the configuration file to specify where database files should be stored.
     * Example: storage_path=/home/user/.crushdb/data/
     */
    public static final String STORAGE_PATH_FIELD = "storage_path=";

    public static boolean init() {

        boolean isInit = false;
        boolean base = createDirectory(BASE_DIR);
        boolean log = createDirectory(LOG_DIR);
        boolean data = createDirectory(DATA_DIR);
        boolean wal = createDirectory(WAL_DIR);
        boolean certs = createDirectory(CU_CA_CERT_PATH);

        if (base && log && data && wal && certs) {
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
            writer.write(String.format("%s%s\n", TLS_ENABLED_FIELD, TLS_ENABLED));
            writer.write(String.format("%s%d\n", TOMBSTONE_GRACE_PERIOD_MS_FIELD, TOMBSTONE_GRACE_PERIOD_MS));
            writer.write(String.format("%s%s\n", STORAGE_PATH_FIELD, DATA_DIR));
            writer.write(String.format("%s%s\n", CU_CA_CERT_PATH_FIELD, CU_CA_CERT_PATH));
            writer.write(String.format("%s%s\n", CA_CERT_PATH_FIELD, CA_CERT_PATH));
            System.out.println("CrushDB Configuration file spawned: " + CONFIGURATION_FILE);
            return true;
        }  catch (IOException e) {
            System.err.println("Error spawning CrushDB configuration: " + e.getLocalizedMessage());
            return false;
        }
    }
}
