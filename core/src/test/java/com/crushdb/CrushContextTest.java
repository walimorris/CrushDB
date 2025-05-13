package com.crushdb;

import com.crushdb.storageengine.config.ConfigManager;
import org.junit.jupiter.api.Test;

class CrushContextTest {

    @Test
    void fromProperties() {
        CrushContext context = ConfigManager.loadContext();
        if (context != null) {
            System.out.println(context.toProperties());
        }
    }

    @Test
    void toProperties() {

    }
}