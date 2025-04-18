package com.crushdb.storageengine.config;

import com.crushdb.DatabaseInitializer;
import com.crushdb.queryengine.QueryEngine;
import com.crushdb.storageengine.StorageEngine;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StorageConfigurationTest {

    @Test
    public void initDefaultConfigurationTest() {
        Properties crushDBProperties = DatabaseInitializer.init();
        StorageEngine storageEngine = DatabaseInitializer.getStorageEngine();
        QueryEngine queryEngine = DatabaseInitializer.getQueryEngine();

        assertNotNull(storageEngine);
        assertNotNull(queryEngine);
        assertNotNull(crushDBProperties);
    }
}