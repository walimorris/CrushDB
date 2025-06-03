package com.crushdb.ui.providers;

import com.crushdb.core.providers.StaticResourceProvider;

import java.io.IOException;
import java.io.InputStream;

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
            }
            return stream;
        } catch (IOException e) {
            System.out.println("Error fetching resource at path: " + e.getMessage());
        }
        return null;
    }
}
