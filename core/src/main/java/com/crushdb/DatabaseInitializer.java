package com.crushdb;

import com.crushdb.index.BPTreeIndexManager;
import com.crushdb.logger.CrushDBLogger;
import com.crushdb.model.crate.CrateManager;
import com.crushdb.queryengine.QueryEngine;
import com.crushdb.queryengine.executor.QueryExecutor;
import com.crushdb.queryengine.parser.QueryParser;
import com.crushdb.queryengine.planner.QueryPlanner;
import com.crushdb.storageengine.StorageEngine;
import com.crushdb.storageengine.config.ConfigManager;
import com.crushdb.storageengine.journal.JournalManager;
import com.crushdb.storageengine.page.PageManager;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static com.crushdb.storageengine.config.ConfigManager.*;

public class DatabaseInitializer {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(DatabaseInitializer.class);

    /**
     * A static instance of the StorageEngine used for managing storage operations.
     */
    private static StorageEngine storageEngine;

    private static String configurationDirectory;

    /**
     * A static instance of {@code QueryEngine} used to manage the lifecycle
     * and operations of the query processing pipeline within CrushDB system.
     * It serves as the centralized component for handling query parsing, planning,
     * and execution tasks, facilitating seamless integration with storage layers
     * and ensuring optimized query performance.
     */
    private static QueryEngine queryEngine;

    private DatabaseInitializer() {}

    public static Properties init(boolean isTest) throws IllegalStateException {
        Properties properties;
        if (isTest) {
            return createTestEnvironment();
        }
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
            properties = ConfigManager.loadConfig();
            storageEngine = createStorageEngine(properties);
            queryEngine = createQueryEngine(properties);
        } else {
            throw new IllegalStateException("Database directory structure failed to initialize.");
        }
        return properties;
    }

    /**
     * Get {@code StorageEngine}
     *
     * @return {@link StorageEngine}
     */
    public static StorageEngine getStorageEngine() {
        return storageEngine;
    }

    /**
     * Get {@code QueryEngine}
     *
     * @return {@link QueryEngine}
     */
    public static QueryEngine getQueryEngine() {
        return queryEngine;
    }

    private static Properties createTestEnvironment() {
        Properties properties;
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
            properties = ConfigManager.loadTestConfig();
            storageEngine = createStorageEngine(properties);
            queryEngine = createQueryEngine(properties);
        } else {
            throw new IllegalStateException("Database directory structure failed to initialize.");
        }
        return properties;
    }

    private static StorageEngine createTestStorageEngine(Properties properties) {
        if (storageEngine == null) {
            PageManager pageManager = PageManager.getInstance();
            pageManager.loadAllPagesOnStartup();
            BPTreeIndexManager indexManager = BPTreeIndexManager.getInstance();
            JournalManager journalManager = JournalManager.getInstance();
            StorageEngine engine = new StorageEngine(pageManager, indexManager, journalManager);
            indexManager.loadIndexesFromDisk(engine, properties);
            return engine;
        }
        return storageEngine;
    }

    /**
     * Creates and initializes a new instance of {@code StorageEngine} if it does not exist.
     * Otherwise, returns the {@code StorageEngine}
     *
     * @return a {@code StorageEngine} instance initialized with a
     * {@code PageManager}, {@code BPTreeIndexManager}, and {@code JournalManager}.
     */
    private static StorageEngine createStorageEngine(Properties properties) {
        if (storageEngine == null) {
            PageManager pageManager = PageManager.getInstance();
            pageManager.loadAllPagesOnStartup();
            BPTreeIndexManager indexManager = BPTreeIndexManager.getInstance();
            JournalManager journalManager = JournalManager.getInstance();
            StorageEngine engine = new StorageEngine(pageManager, indexManager, journalManager);
            indexManager.loadIndexesFromDisk(engine, properties);
            return engine;
        }
        return storageEngine;
    }

    /**
     * Creates and initializes a new instance of {@code QueryEngine} if it does not exist.
     * The method ensures that the required {@code StorageEngine} is initialized before
     * constructing the {@code QueryEngine}. The initialization process includes:
     * <ol>
     *     <li>Creating a {@code StorageEngine} if it is not already available.</li>
     *     <li>Initializing the {@code CrateManager} with the storage engine.</li>
     *     <li>Creating and combining the required components: {@code QueryParser}, {@code QueryPlanner}, and {@code QueryExecutor}.</li>
     * </ol>
     *
     * @return a {@code QueryEngine} instance responsible for query processing, including parsing,
     *         planning, and execution. If a {@code QueryEngine} is already initialized, the existing instance is returned.
     */
    private static QueryEngine createQueryEngine(Properties properties) {
        if (storageEngine == null) {
            storageEngine = createStorageEngine(properties);
        }

        if (queryEngine == null) {
            CrateManager.init(storageEngine);
            CrateManager crateManager = CrateManager.getInstance();
            crateManager.loadCratesFromDisk();

            QueryParser queryParser = new QueryParser();
            QueryPlanner queryPlanner = new QueryPlanner(crateManager);
            QueryExecutor queryExecutor = new QueryExecutor();
            return new QueryEngine(queryParser, queryPlanner, queryExecutor);
        }
        return queryEngine;
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
