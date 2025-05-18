package com.crushdb.storageengine.config;

import com.crushdb.bootstrap.DatabaseInitializer;
import com.crushdb.queryengine.QueryEngine;
import com.crushdb.storageengine.StorageEngine;
import com.crushdb.utils.FileUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StorageConfigurationTest {

    @BeforeAll
    public static void setUp() {
        FileUtil.spawnParentTestDatabaseDirectory();
    }

    @AfterAll
    public static void tearDown() {
        FileUtil.destroyTestDatabaseDirectory();
    }

    @Test
    public void initDefaultConfigurationTest() {
        Properties crushDBProperties = DatabaseInitializer.init(true);
        StorageEngine storageEngine = DatabaseInitializer.getStorageEngine();
        QueryEngine queryEngine = DatabaseInitializer.getQueryEngine();

        assertNotNull(storageEngine);
        assertNotNull(queryEngine);
        assertNotNull(crushDBProperties);
    }
}