package com.crushdb.model.crate;

import com.crushdb.index.btree.SortOrder;
import com.crushdb.logger.CrushDBLogger;
import com.crushdb.model.document.BsonType;
import com.crushdb.storageengine.StorageEngine;
import com.crushdb.storageengine.config.ConfigManager;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CrateManager {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(CrateManager.class);

    private static CrateManager instance;

    private static Properties properties;

    private Map<String, Crate> crateRegistry;

    private final StorageEngine storageEngine;

    private CrateManager(StorageEngine storageEngine) {
        this.crateRegistry = new HashMap<>();
        this.storageEngine = storageEngine;
    }

    public static void reset() {
        if (instance != null) {
            instance.crateRegistry.clear();
        }
        instance = null;
        properties = null;
    }

    /**
     * Initializes the CrateManager singleton instance with the provided StorageEngine.
     * This method must be called before using any other methods in the CrateManager.
     * If the instance is already initialized, no action will be taken.
     *
     * @param storageEngine the storage engine to be used by the CrateManager
     */
    public static synchronized void init(StorageEngine storageEngine) {
        if (instance == null) {
            instance = new CrateManager(storageEngine);
        }
    }

    /**
     * Retrieves the singleton instance of the CrateManager. If the CrateManager
     * has not been initialized prior to calling this method, an {@code IllegalArgumentException}
     * will be thrown.
     *
     * @return the singleton instance of the CrateManager
     *
     * @throws IllegalArgumentException if the CrateManager has not been initialized
     */
    public static CrateManager getInstance(Properties props) throws IllegalArgumentException {
        if (instance == null) {
            LOGGER.error("Attempt to utilize CrateManager without initialization. Init before use.",
                    IllegalArgumentException.class.getName());
            throw new IllegalArgumentException("Attempt to utilize uninitialized CrateManager. Init CrateManager before use.");
        }
        properties = props;
        return instance;
    }

    /**
     * Creates a new crate with the specified name and registers it with the crate registry.
     * If a crate with the given name already exists, an {@code IllegalArgumentException} is thrown.
     *
     * @param crateName the name of the crate to be created
     *
     * @return the newly created {@code Crate} instance
     *
     * @throws IllegalArgumentException if the provided crate already exists
     */
    public Crate createCrate(String crateName) throws IllegalArgumentException {
        if (crateRegistry.containsKey(crateName)) {
            LOGGER.error("Crate already exists: " + crateName, IllegalArgumentException.class.getName());
            throw new IllegalArgumentException("Crate already exist: " + crateName);
        }
        Crate crate = new Crate(crateName, storageEngine);
        crateRegistry.put(crateName, crate);

        String cratesDir = properties.getProperty(ConfigManager.CRATES_DIR_FIELD);

        // build indexesDir based on environment
        if (Boolean.parseBoolean(properties.getProperty("isTest"))) {
            cratesDir = cratesDir.replace("~/", properties.getProperty("baseDir"))
                    .replace("/tmp/.crushdb/", "/tmp/");
        }

        // persist crate
        crate.serialize(cratesDir + crateName + ".crate");
        //create the _id index on the crate immediately - TODO: establish order practices on id_indexes
        this.storageEngine.createIndex(BsonType.LONG, crateName, "id_index", "_id", false, 3, SortOrder.ASC);
        return crate;
    }

    /**
     * Retrieves a crate by its name from the crate registry.
     * If the specified crate does not exist, an error is logged, and an {@code IllegalArgumentException} is thrown.
     *
     * @param crateName the name of the crate to be retrieved
     *
     * @return the {@code Crate} instance associated with the provided name
     *
     * @throws IllegalArgumentException if the crate does not exist in the registry
     */
    public Crate getCrate(String crateName) throws IllegalArgumentException {
        Crate crate = this.crateRegistry.get(crateName);
        if (crate == null) {
            LOGGER.error("Crate does not exist: " + crateName, IllegalArgumentException.class.getName());
            throw new IllegalArgumentException("Crate does not exist: " + crateName);
        }
        return crate;
    }

    /**
     * Retrieves an unmodifiable view of all crates currently registered in the system.
     * The returned map contains crate names as keys and their corresponding {@code Crate} objects as values.
     *
     * @return an unmodifiable map of crate names to {@code Crate} instances
     */
    public Map<String, Crate> getAllCrates() {
        return Collections.unmodifiableMap(this.crateRegistry);
    }

    /**
     * Deletes the crate with the specified name from the crate registry.
     * If the crate exists, it is removed from the registry and the method returns true.
     * If the crate does not exist, no action is taken and the method returns false.
     *
     * @param crateName the name of the crate to be deleted
     *
     * @return true if the crate was successfully removed from the registry, false otherwise
     */
    public boolean deleteCrate(String crateName) {
        Crate crate = getCrate(crateName);
        if (crate != null) {
            this.crateRegistry.remove(crate.getName());
            return true;
        }
        return false;
    }

    /**
     * Checks if a crate with the specified name exists in the crate registry.
     *
     * @param crateName the name of the crate to check for existence
     *
     * @return true if a crate with the specified name exists, false otherwise
     */
    public boolean exists(String crateName) {
        return this.crateRegistry.containsKey(crateName);
    }

    public void loadCratesFromDisk() {
        String cratesDir = properties.getProperty(ConfigManager.CRATES_DIR_FIELD);

        // build indexesDir based on environment
        if (Boolean.parseBoolean(properties.getProperty("isTest"))) {
            cratesDir = cratesDir.replace("~/", properties.getProperty("baseDir"))
                    .replace("/tmp/.crushdb/", "/tmp/");
        }
        // pull if exists
        Path cratesPath = Path.of(cratesDir);
        if (Files.exists(cratesPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(cratesPath, "*.crate")) {
                for (Path crateFile : stream) {
                    Crate crate = Crate.deserialize(crateFile, storageEngine);
                    this.crateRegistry.put(crate.getName(), crate);
                }
            } catch (IOException e) {
                LOGGER.error("Failed to load crates from disk: " + e.getMessage(), IOException.class.getName());
            }
        }
    }
}
