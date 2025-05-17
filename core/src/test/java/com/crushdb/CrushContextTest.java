package com.crushdb;

import com.crushdb.storageengine.config.ConfigManager;
import com.crushdb.utils.FileUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.Properties;

class CrushContextTest {

    @Test
    void prodContextTest() {
        CrushContext crushContext = ConfigManager.loadContext();
        Assertions.assertNotNull(crushContext);
        Assertions.assertFalse(Boolean.parseBoolean(crushContext.getProperty("isTest")));
        Assertions.assertFalse(crushContext.getProperty(CrushContext.BASE_DIR).contains("/tmp"));
        System.out.println(crushContext);
    }

//    @Test
//    void prodContextFromPropertiesTest() {
//        CrushContext cxt = new CrushContext();
//        Properties properties = new Properties();
//        properties.setProperty("walPath", "/Users/walimorris/.crushdb/wal/crushdb.journal",
//                pageSize=4096,
//                configPath=/Users/walimorris/.crushdb/crushdb.conf,
//                logMaxSizeMb=50, cratesPath=/Users/walimorris/.crushdb/data/crates/,
//                caCertPath=/etc/ssl/certs/ca-certificates.crt,
//                dataPath=/Users/walimorris/.crushdb/data/,
//                baseDir=/Users/walimorris/.crushdb/,
//                eagerLoadPages=true,
//                tlsEnabled=false,
//                logLevel=INFO,ERROR,
//                autoCompressOnInsert=false,
//                walDirectory=/Users/walimorris/.crushdb/wal/,
//                walEnabled=true, cacheMemoryLimitMb=32,
//                indexesPath=/Users/walimorris/.crushdb/data/indexes/,
//                storagePath=/Users/walimorris/.crushdb/data/crushdb.db,
//                customCaCertPath=~/.crushdb/certs/,
//                tombstoneGc=60000,
//                cacheMaxPages=8192,
//                metaFilePath=/Users/walimorris/.crushdb/data/meta.dat,
//                logDirectory=/Users/walimorris/.crushdb/log/,
//                logMaxFiles=10,
//                isTest=false,
//                logRetentionDays=7
//    }

    @Test
    void testContextToPropertiesTest() {
        // tmp needs to exist, breakdown after
        FileUtil.spawnParentTestDatabaseDirectory();
        CrushContext testCrushContext = ConfigManager.loadTestContext();
        Assertions.assertNotNull(testCrushContext);
        Assertions.assertTrue(Boolean.parseBoolean(testCrushContext.getProperty("isTest")));
        FileUtil.destroyTestDatabaseDirectory();
        System.out.println(testCrushContext);
    }
}