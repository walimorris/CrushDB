package com.crushdb;

import com.crushdb.bootstrap.ConfigManager;
import com.crushdb.bootstrap.CrushContext;
import com.crushdb.utils.FileUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CrushContextTest {

    @Test
    void prodContextTest() {
        CrushContext crushContext = ConfigManager.loadContext();
        Assertions.assertNotNull(crushContext);
        Assertions.assertFalse(Boolean.parseBoolean(crushContext.getProperty("isTest")));
        Assertions.assertFalse(crushContext.getProperty(CrushContext.BASE_DIR).contains("/tmp"));
        System.out.println(crushContext);
    }

    @Test
    void testContextToPropertiesTest() {
        // tmp needs to exist, breakdown after
        FileUtil.spawnParentTestDatabaseDirectory();
        CrushContext testCrushContext = ConfigManager.loadTestContext();
        Assertions.assertNotNull(testCrushContext);
        Assertions.assertTrue(Boolean.parseBoolean(testCrushContext.getProperty("isTest")));
        Assertions.assertTrue(testCrushContext.getProperty(CrushContext.BASE_DIR).contains("/tmp"));
        FileUtil.destroyTestDatabaseDirectory();
        System.out.println(testCrushContext);
    }
}