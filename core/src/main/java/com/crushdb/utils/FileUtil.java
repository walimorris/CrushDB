package com.crushdb.utils;

import com.crushdb.storageengine.config.ConfigManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

public class FileUtil {
    public static final Path TEST_DB_DIR = Paths.get(ConfigManager.TEST_BASE_DIR);

    /**
     * Test database files are created in the (user.home)/.crushdb/ directory
     * inside the tmp directory. To ensure these files are cleaned up, this
     * cleaning method destroys and deletes the tmp directory and all files
     * inside it.
     */
    public static void cleanTestDatabaseDirectory() {
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
    }
}
