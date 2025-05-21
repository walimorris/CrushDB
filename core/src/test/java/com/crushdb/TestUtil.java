package com.crushdb;

import com.crushdb.core.bootstrap.ConfigManager;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class TestUtil {
    public static final Path TEST_DB_DIR = Paths.get(ConfigManager.TEST_BASE_DIR);

    public static Properties loadTestProperties() {
        Properties properties = new Properties();
        // TODO: we will split test properties with actual properties here instead of production code
        return properties;
    }
}
