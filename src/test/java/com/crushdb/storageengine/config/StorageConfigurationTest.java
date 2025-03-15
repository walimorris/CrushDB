package com.crushdb.storageengine.config;

import com.crushdb.core.DatabaseInitializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StorageConfigurationTest {

    @Test
    public void initDefaultConfigurationTest() {
        Properties crushDBProperties = DatabaseInitializer.init();
        assertNotNull(crushDBProperties);
    }
}