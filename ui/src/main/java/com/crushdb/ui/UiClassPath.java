package com.crushdb.ui;

import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;

public class UiClassPath {
    public static void main(String[] args) throws Exception {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Enumeration<URL> urls = loader.getResources("web/index.html");

        while (urls.hasMoreElements()) {
            System.out.println("Found at: " + urls.nextElement());
        }

        InputStream in = loader.getResourceAsStream("web/index.html");
        System.out.println("Found InputStream? " + (in != null));
    }
}
