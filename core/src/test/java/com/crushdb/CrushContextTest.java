package com.crushdb;

import com.crushdb.storageengine.config.ConfigManager;
import com.crushdb.utils.FileUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.Properties;

class CrushContextTest {

    @Test
    void prodContextToPropertiesTest() {
        Properties prodProperties = Objects.requireNonNull(ConfigManager.loadContext()).toProperties();
        Assertions.assertNotNull(prodProperties);
        Assertions.assertFalse(Boolean.parseBoolean(prodProperties.getProperty("isTest")));
        System.out.println(prodProperties);
    }

    @Test
    void testContextToPropertiesTest() {
        // tmp needs to exist, breakdown after
        FileUtil.spawnParentTestDatabaseDirectory();
        Properties testProperties = Objects.requireNonNull(ConfigManager.loadTestContext()).toProperties();
        Assertions.assertNotNull(testProperties);
        Assertions.assertTrue(Boolean.parseBoolean(testProperties.getProperty("isTest")));
        FileUtil.destroyTestDatabaseDirectory();
        System.out.println(testProperties);
    }
}