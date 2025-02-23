package com.crushdb.storageengine.config;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class StorageConfigurationTest {

    @Test
    public void initDefaultConfigurationTest() {
        boolean initCrushDBConfig = StorageConfiguration.init();
        assertTrue(initCrushDBConfig);
    }
}