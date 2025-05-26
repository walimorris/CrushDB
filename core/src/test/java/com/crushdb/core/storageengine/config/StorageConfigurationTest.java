package com.crushdb.core.storageengine.config;

import com.crushdb.core.bootstrap.DatabaseInitializer;
import com.crushdb.core.bootstrap.TestCrushContext;
import com.crushdb.core.index.BPTreeIndexManager;
import com.crushdb.core.model.crate.CrateManager;
import com.crushdb.core.storageengine.journal.JournalManager;
import com.crushdb.core.storageengine.page.PageManager;
import com.crushdb.core.utils.FileUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StorageConfigurationTest {

    @BeforeAll
    public static void setUp() {
        FileUtil.spawnParentTestDatabaseDirectory();
        PageManager.reset();
        BPTreeIndexManager.reset();
        JournalManager.reset();
        CrateManager.reset();
    }

    @AfterAll
    public static void tearDown() {
        FileUtil.destroyTestDatabaseDirectory();
    }

    @Test
    public void initDefaultConfigurationTest() {
        TestCrushContext cxt = DatabaseInitializer.initTest();
        assertNotNull(cxt);
        Assertions.assertNotNull(cxt.getPageManager());
        Assertions.assertTrue(cxt.getPageManager().getDataFile().toAbsolutePath().toString().contains("/tmp"));
        Assertions.assertNotNull(cxt.getStorageEngine());
        Assertions.assertNotNull(cxt.getIndexManager());
        Assertions.assertNotNull(cxt.getJournalManager());
        Assertions.assertNotNull(cxt.getCrateManager());
        Assertions.assertNotNull(cxt.getQueryParser());
        Assertions.assertNotNull(cxt.getQueryPlanner());
        Assertions.assertNotNull(cxt.getQueryExecutor());
        Assertions.assertNotNull(cxt.getQueryEngine());
    }
}