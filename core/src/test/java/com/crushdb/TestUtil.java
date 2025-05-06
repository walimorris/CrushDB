package com.crushdb;

import com.crushdb.storageengine.config.ConfigManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Properties;
import java.util.stream.Stream;

public class TestUtil {
    public static final Path TEST_DB_DIR = Paths.get(ConfigManager.TEST_BASE_DIR);

    public static Properties loadTestProperties() {
        Properties properties = new Properties();
        // TODO: we will split test properties with actual properties here instead of production code
        return properties;
    }

    public static void cleanTestDir() {
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
                throw new RuntimeException("Failed to clean test dir", e);
            }
        }
    }
}
