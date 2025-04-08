package com.crushdb.model.crate;

import com.crushdb.logger.CrushDBLogger;
import com.crushdb.storageengine.StorageEngine;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CrateManager {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(CrateManager.class);

    private static CrateManager instance;

    private final Map<String, Crate> crateRegistry;

    private final StorageEngine storageEngine;

    private CrateManager(StorageEngine storageEngine) {
        this.crateRegistry = new HashMap<>();
        this.storageEngine = storageEngine;
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
    public static CrateManager getInstance() throws IllegalArgumentException {
        if (instance == null) {
            LOGGER.error("Attempt to utilize CrateManager without initialization. Init before use.",
                    IllegalArgumentException.class.getName());
            throw new IllegalArgumentException("Attempt to utilize uninitialized CrateManager. Init CrateManager before use.");
        }
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
        if (this.crateRegistry.containsKey(crateName)) {
            LOGGER.error("Crate already exists: " + crateName, IllegalArgumentException.class.getName());
            throw new IllegalArgumentException("Crate already exist: " + crateName);
        }
        Crate crate = new Crate(crateName, storageEngine);
        crateRegistry.put(crateName, crate);
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
}
