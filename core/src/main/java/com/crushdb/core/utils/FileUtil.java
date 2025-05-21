package com.crushdb.utils;

import com.crushdb.bootstrap.ConfigManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

public class FileUtil {
    public static final Path TEST_DB_DIR = Paths.get(ConfigManager.TEST_BASE_DIR);

    /**
     * Creates a temporary parent test database directory for storing test database files.
     * The directory is created at the path defined by the constant {@code TEST_DB_DIR}.
     * <p>
     * This method is typically used in the context of testing to set up the required
     * temporary environment for database-related tests.
     * <p>
     * Note: The created directory is not automatically cleaned up and should be
     * explicitly deleted after use.
     *
     * @see ConfigManager
     */
    public static void spawnParentTestDatabaseDirectory() {
        try {
            Path parentTmpTestDirectory = Files.createDirectory(TEST_DB_DIR);
            System.out.println("Spawned: Temporary Test DB directory at: " + parentTmpTestDirectory.toAbsolutePath());
        } catch (IOException e) {
            System.out.println("Error creating DB test directory at: " + TEST_DB_DIR);
        }
    }

    /**
     * Test database files are created in the (user.home)/.crushdb/ directory
     * inside the tmp directory. To ensure these files are cleaned up, this
     * cleaning method destroys and deletes the tmp directory and all files
     * inside it.
     *
     * @see ConfigManager
     */
    public static void destroyTestDatabaseDirectory() throws RuntimeException {
        if (Files.exists(TEST_DB_DIR)) {
            try (Stream<Path> walk = Files.walk(TEST_DB_DIR)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(file -> {
                            if (!file.delete()) {
                                System.out.println("Failed to delete: " + file.getAbsolutePath());
                            }
                        });
            } catch (IOException e) {
                throw new RuntimeException("Failed to clean test directory", e);
            }
        }
        System.out.println("Destroyed: Temporary Test DB at: " + TEST_DB_DIR);
    }
}
