package com.crushdb.ui.providers;

import com.crushdb.core.providers.StaticResourceProvider;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.*;

class UiStaticResourceProviderTest {

    @Test
    void getResource() throws IOException {
        ServiceLoader<StaticResourceProvider> serviceLoader = ServiceLoader.load(StaticResourceProvider.class);
        StaticResourceProvider provider = serviceLoader.findFirst().orElseThrow(() -> new IllegalStateException("NO_FOUND_STATIC_RESOURCE_PROVIDER"));
        try (InputStream in = provider.getResource("web/signin.html")) {
            assertEquals(UiStaticResourceProvider.class, provider.getClass());
            assertNotNull(in, "Expected web/signin.html to be available via StaticResourceProvider");
        }
    }
}