package com.crushdb.storageengine.page;

import com.crushdb.storageengine.config.ConfigManager;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

public class PageManager {

    /**
     * Represents the file path to the data file used by the crushdb.
     */
    private final Path dataFile = Paths.get(ConfigManager.get(ConfigManager.DATABASE_FILE, null));

    /**
     * Defines the size of each page in bytes within the system.
     * The value for this variable is retrieved from the application's configuration file,
     * This value is crucial for determining the memory management behavior of the system,
     * particularly regarding the number of pages that can be cached and the calculation
     * of memory limits for storage operations.
     */
    private final int pageSize = ConfigManager.getInt(ConfigManager.PAGE_SIZE_FIELD,  4096);

    /**
     * Represents the maximum number of pages that can be cached.
     *
     * This value is determined by the `calculateMaxPage` method and is based on either:
     * - A configured memory limit in megabytes, which is converted into the maximum number of pages using the page size.
     * - A default or explicitly configured maximum number of cacheable pages if the memory limit is not specified.
     */
    private final int maxPages = calculateMaxPage();

    /**
     * The load factor determines the threshold for resizing operations in hash-based data structures.
     * It represents the ratio of the number of elements to the capacity of the structure before resizing occurs.
     * A higher load factor reduces memory overhead but increases the likelihood of collisions,
     * while a lower load factor improves lookup performance at the expense of using more memory.
     *
     * In the context of this class, the load factor is a fixed value used for performance optimization
     * and balancing memory consumption against lookup and insertion efficiency.
     */
    private final float loadFactor = 0.75f;

    /**
     * Represents the initial capacity of the page cache, calculated based on the
     * maximum number of pages and the load factor.
     *
     * This value is derived by dividing the maximum number of pages by the load factor
     * and rounding up to the nearest whole number. It is used to initialize the cache
     * with an optimal size to accommodate the expected number of cacheable pages
     * while maintaining efficient performance. Generally, the idea is to reduce or
     * remove reloads, while relying on LRU functionality.
     */
    private final int initialCapacity = (int) Math.ceil(maxPages / loadFactor);

    /**
     * A thread-safe, Least Recently Used (LRU) cache implementation backed by a {@link LinkedHashMap}.
     * This cache stores pages in memory with a maximum capacity defined by the configuration.
     *
     * The cache leverages access-order iteration to keep track of the most recently accessed elements.
     * When the size of the cache exceeds the maximum number of pages, the least recently accessed entry
     * is automatically removed to retain the maximum page limit.
     *
     * Key characteristics:
     * - The key represents the unique identifier of a page.
     * - The value is the {@link Page} object corresponding to the key.
     * - It employs a load factor of 0.75 and an access-based reordering mechanism.
     * - Automatically evicts the least recently accessed page when the cache exceeds its maximum capacity.
     *
     * This cache enables efficient management of in-memory pages.
     */
    private final Map<Long, Page> cache = new LinkedHashMap<>(initialCapacity, .75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, Page> eldest) {
            // TODO: add persistence logic
            return size() > maxPages;
        }
    };

    /**
     * Calculates the maximum number of pages that can be cached based on the memory limit
     * or default maximum pages configuration.
     *
     * If a memory limit is specified in megabytes via the configuration, the method
     * calculates the maximum number of pages by dividing the memory limit in bytes
     * by the page size. If the memory limit is not specified or commented out, it uses
     * a default or configured value for the maximum number of pages.
     *
     * @return the maximum number of cacheable pages, as determined by the memory limit or default configuration
     */
    private int calculateMaxPage() {
        int memoryLimitMB = ConfigManager.getInt(ConfigManager.CACHE_MEMORY_LIMIT_MB_FIELD, -1);
        // if maxLimitMV  <= 0 that means cache_memory_limit_mb is not commented out and cache_max_pages should be used
        if (memoryLimitMB > 0) {
            // this will calculate the number of pages in mb (32 * 1024 * 1024 / 4096)
            // if default values are used (32 mb and 4kB page size) the result
            // is 8192 pages maximum in cache
            long memoryLimitBytes = memoryLimitMB * 1024L * 1024L;
            return (int) (memoryLimitBytes / this.pageSize);
        }
        return ConfigManager.getInt(ConfigManager.CACHE_MAX_PAGES_FIELD, 8192);
    }
}
