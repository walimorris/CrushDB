package com.crushdb;

import com.crushdb.storageengine.config.ConfigManager;
import org.junit.jupiter.api.Test;

class CrushContextTest {

    @Test
    void fromProperties() {
        CrushContext prodContext = ConfigManager.loadContext();
        if (prodContext != null) {
            System.out.println(prodContext.toProperties());
        }

        CrushContext testContext = ConfigManager.loadTestContext();
        if (testContext != null) {
            System.out.println(testContext.toProperties());
        } else {
            System.out.println("TestContext is not available!");
        }
    }

    @Test
    void toProperties() {

    }
}