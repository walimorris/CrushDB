package com.crushdb.ui.providers;

import com.crushdb.core.providers.StaticResourceProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.ServiceLoader;

/**
 * {@code UiStaticResourceProvider} fetches static resources from this module's resource directory.
 * However, utilizing a {@linkplain  ServiceLoader ServiceLoader} can provide this specific provider
 * in other {@code CrushDB} modules that need access to this module's resources.
 */
public class UiStaticResourceProvider implements StaticResourceProvider {

    @Override
    public InputStream getResource(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(path)) {
            System.out.println("searching path: " + path);
            if (stream == null) {
                System.out.println("Resource not found: " + path);
            } else {
                System.out.println("Resource found: " + stream.getClass().getName());
            }
            return stream;
        } catch (IOException e) {
            System.out.println("Error fetching resource at path: " + e.getMessage());
        }
        return null;
    }
}
