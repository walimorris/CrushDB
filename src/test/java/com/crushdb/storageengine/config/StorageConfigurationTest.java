package com.crushdb.storageengine.config;

import com.crushdb.core.DatabaseInitializer;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class StorageConfigurationTest {

    @Test
    public void initDefaultConfigurationTest() {
        boolean initCrushDB = DatabaseInitializer.init();
        assertTrue(initCrushDB);
    }
}