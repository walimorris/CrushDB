package com.crushdb.core;

import com.crushdb.core.bootstrap.ConfigManager;
import com.crushdb.core.bootstrap.CrushContext;
import com.crushdb.core.bootstrap.TestCrushContext;
import com.crushdb.core.index.BPTreeIndexManager;
import com.crushdb.core.model.crate.CrateManager;
import com.crushdb.core.storageengine.journal.JournalManager;
import com.crushdb.core.storageengine.page.PageManager;
import com.crushdb.core.utils.FileUtil;
import org.junit.jupiter.api.*;

class CrushContextTest {

    @BeforeAll
    public static void setUp() {
        FileUtil.spawnParentTestDatabaseDirectory();
    }

    @AfterAll
    public static void tearDown() {
        FileUtil.destroyTestDatabaseDirectory();
    }

    @BeforeEach
    public void reset() {
        PageManager.reset();
        BPTreeIndexManager.reset();
        JournalManager.reset();
        CrateManager.reset();
    }

    @Test
    void prodContextTest() {
        CrushContext cxt = ConfigManager.loadContext();
        Assertions.assertNotNull(cxt);
        Assertions.assertFalse(Boolean.parseBoolean(cxt.getProperty("isTest")));
        Assertions.assertFalse(cxt.getProperty(CrushContext.BASE_DIR).contains("/tmp"));

        // ensure we have the managers and engines we need
        Assertions.assertEquals(8082, cxt.getPort());
        Assertions.assertNotNull(cxt.getPageManager());
        Assertions.assertFalse(cxt.getPageManager().getDataFile().toAbsolutePath().toString().contains("/tmp"));
        Assertions.assertNotNull(cxt.getStorageEngine());
        Assertions.assertNotNull(cxt.getIndexManager());
        Assertions.assertNotNull(cxt.getJournalManager());
        Assertions.assertNotNull(cxt.getCrateManager());
        Assertions.assertNotNull(cxt.getQueryParser());
        Assertions.assertNotNull(cxt.getQueryPlanner());
        Assertions.assertNotNull(cxt.getQueryExecutor());
        Assertions.assertNotNull(cxt.getQueryEngine());

        System.out.println(cxt);
    }

    @Test
    void testContextToPropertiesTest() {
        // tmp needs to exist, breakdown after
        TestCrushContext testCxt = ConfigManager.loadTestContext();
        Assertions.assertNotNull(testCxt);
        Assertions.assertTrue(Boolean.parseBoolean(testCxt.getProperty("isTest")));
        Assertions.assertTrue(testCxt.getProperty(CrushContext.BASE_DIR).contains("/tmp"));
        Assertions.assertTrue(testCxt.getDataPath().contains("/tmp"));
        FileUtil.destroyTestDatabaseDirectory();

        Assertions.assertEquals(8082, testCxt.getPort());
        Assertions.assertNotNull(testCxt.getPageManager());
        Assertions.assertTrue(testCxt.getPageManager().getDataFile().toAbsolutePath().toString().contains("/tmp"));
        Assertions.assertNotNull(testCxt.getStorageEngine());
        Assertions.assertNotNull(testCxt.getIndexManager());
        Assertions.assertNotNull(testCxt.getJournalManager());
        Assertions.assertNotNull(testCxt.getCrateManager());
        Assertions.assertNotNull(testCxt.getQueryParser());
        Assertions.assertNotNull(testCxt.getQueryPlanner());
        Assertions.assertNotNull(testCxt.getQueryExecutor());
        Assertions.assertNotNull(testCxt.getQueryEngine());

        System.out.println(testCxt);
    }
}