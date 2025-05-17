package com.crushdb.logger;

import com.crushdb.bootstrap.ConfigManager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

public class CrushDBLogger {
    private static int maxLogFiles;
    private static int maxLogRetentionDays;
    private static long maxLogSize;
    private static String logLevel;

    private static final String LOG_DIRECTORY = ConfigManager.LOG_DIR;
    private static final String LOG_FILE = LOG_DIRECTORY + "crushdb.log";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Pattern LOG_PATTERN = Pattern.compile("crushdb\\.log\\.(\\d+)$");
    private static final String INFO = "INFO";
    private static final String ERROR = "ERROR";

    private static final ReentrantLock lock = new ReentrantLock();

    static {
        loadConfiguration();
    }

    private final String className;

    private CrushDBLogger(Class<?> clazz) {
        this.className = clazz.getSimpleName();
    }

    /**
     * Factory method to retrieve a logger instance for a given class.
     *
     * @param clazz The class requesting a logger.
     * @return A CrushDBLogger instance for the class.
     */
    public static CrushDBLogger getLogger(Class<?> clazz) {
        return new CrushDBLogger(clazz);
    }

    private static void loadConfiguration() {
        Properties properties = ConfigManager.loadConfig();
        if (properties != null) {
            maxLogFiles = parseInt(properties.getProperty(ConfigManager.LOG_MAX_FILES, "5"));
            maxLogRetentionDays = parseInt(properties.getProperty(ConfigManager.LOG_RETENTION_DAYS_FIELD, "7"));
            maxLogSize = parseLong(properties.getProperty(ConfigManager.LOG_MAX_SIZE_MB_FIELD, "50")) * 1024 * 1024;
            logLevel = properties.getProperty(ConfigManager.LOG_LEVEL, "INFO");
        } else {
            System.err.println("Error reading configuration. Using default log settings.");
        }
    }

    public void info(String message, String exception) {
        if (logLevel.contains(INFO)) {
            log(message, INFO, exception);
        }
    }

    public void error(String message, String exception) {
        if (logLevel.contains(ERROR)) {
            log(message, ERROR, exception);
        }
    }

    private void log(String message, String level, String exception) {
        lock.lock();
        try {
            logRotationCheck();
            String log;
            if (exception != null) {
                log = String.format("[%s] [%s] [%s] [%s] %s%n", LocalDateTime.now().format(FORMATTER), level, className, exception, message);
            } else {
                log = String.format("[%s] [%s] [%s] %s%n", LocalDateTime.now().format(FORMATTER), level, className, message);
            }
            Files.write(Paths.get(LOG_FILE), log.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.printf("Error: cannot write log %s%n", e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    private void logRotationCheck() {
        List<Path> paths = new ArrayList<>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Path.of(LOG_DIRECTORY))) {
            for (Path path : directoryStream) {
                paths.add(path);
            }
        } catch (IOException e) {
            System.out.printf("Error: cannot spawn directory stream on: %s", LOG_DIRECTORY);
        }
        File logFile = new File(LOG_FILE);
        if (logFile.exists() && logFile.length() > this.getMaxLogSize()) {
            rotateLogs(paths);
        }
    }

    private void rotateLogs(List<Path> paths) {
        // current log file can no longer take more data - rotate
        List<Path> reverseOrderedPaths = reverseSortedPaths(paths);
        if (reverseOrderedPaths.size() != paths.size()) {
            throw new IllegalStateException("Sorted log paths is not equal to original paths in: " + LOG_DIRECTORY);
        }
        for (Path path : reverseOrderedPaths) {
            // skip the original, it'll be renamed last
            if (!String.valueOf(path).equals(LOG_FILE)) {
                int suffix = extractLogIndex(String.valueOf(path));
                if (suffix != -1) {
                    Path newPath = null;
                    try {
                        newPath = Path.of(LOG_FILE + "." + (++suffix));
                        if (Files.exists(path)) {
                            Files.move(path, newPath, StandardCopyOption.REPLACE_EXISTING);
                        }
                    } catch (IOException e) {
                        System.out.printf("Error: cannot rotate path: %s -> %s %n", path, newPath);
                    }
                } else {
                    throw new IllegalStateException("Log File rotation corrupted by numbering differences.");
                }
            }
        }
        try {
            Files.move(Path.of(LOG_FILE), Path.of(LOG_FILE + "." + 1));
        } catch (IOException e) {
            System.out.printf("Error: cannot rotate path: %s -> %s %n", Path.of(LOG_FILE), Path.of(LOG_FILE + "." + 1));
        }
    }

    private List<Path> reverseSortedPaths(List<Path> paths) {
        int size = paths.size();
        List<Path> result = new ArrayList<>();

        // we can guarantee accessing array value is O(1) time complexity
        int start = size;
        while (start > 0) {
            String logFile = LOG_FILE + "." + (start);
            result.add(Path.of(logFile));
            start--;
        }
        return result;
    }

    public static int extractLogIndex(String pathStr) {
        Matcher matcher = LOG_PATTERN.matcher(pathStr);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return -1;
    }

    public int getMaxLogRetentionDays() {
        return maxLogRetentionDays;
    }

    public int getMaxLogFiles() {
        return maxLogFiles;
    }

    public long getMaxLogSize() {
        return maxLogSize;
    }

    public String getLogLevel() {
        return logLevel;
    }
}
