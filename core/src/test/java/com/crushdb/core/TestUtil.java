package com.crushdb.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

/**
 * {@code TestUtil} supplies convenient methods that can be used in {@code CrushDB} test core.
 *
 * @author walimorris
 */
public class TestUtil {

    /**
     * Private constructor: prevents instantiation and enforces utility class usage.
     */
    private TestUtil() {
        throw new AssertionError("Cannot instantiate TestUtil!");
    }

    /**
     * Loads resource files in this module and outputs it's content as a string value.
     * Resources are streamed and read using a {@linkplain Scanner scanner}. If this
     * method is given a non-existing resource or if the resource is empty this method
     * will throw a IllegalStateException.
     *
     * <p>This is convenient for static files used for test purposes:
     *
     * <ul>
     *     <li>Json files</li>
     *     <li>Reading content</li>
     *     <li>Ensuring resources remain the same</li>
     *     <li>Other creative reasons</li>
     * </ul>
     *
     * @param name {@link String} name of the resource file
     *
     * @return {@link String} String representation of static resource
     * @throws IllegalStateException if static resource doesn't exist of is empty
     */
    public static String loadResourceAsString(String name) throws IllegalStateException {
        ClassLoader loader = TestUtil.class.getClassLoader();
        Scanner scanner = null;
        StringBuilder builder = new StringBuilder();
        try (InputStream resource = loader.getResourceAsStream(name)) {
            if (resource != null && resource.available() > 0) {
                scanner = new Scanner(resource);
                while (scanner.hasNextLine()) {
                    builder.append(scanner.nextLine());
                }
            } else {
                throw new IllegalStateException("Resource may not exist or may be empty.");
            }
        } catch (IOException e) {
            System.out.printf("Error on resource %s: %s%n", name, e.getMessage());
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return builder.toString();
    }
}
