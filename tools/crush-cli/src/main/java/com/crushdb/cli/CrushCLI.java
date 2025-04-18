package com.crushdb.cli;

import com.crushdb.DatabaseInitializer;
import com.crushdb.queryengine.QueryEngine;
import com.crushdb.storageengine.StorageEngine;

public class CrushCLI {
    private final StorageEngine storageEngine;
    private final QueryEngine queryEngine;

    public CrushCLI() {
        DatabaseInitializer.init();
        storageEngine = DatabaseInitializer.getStorageEngine();
        queryEngine = DatabaseInitializer.getQueryEngine();
    }
}
