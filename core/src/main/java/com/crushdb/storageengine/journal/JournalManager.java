package com.crushdb.storageengine.journal;

import com.crushdb.logger.CrushDBLogger;
import com.crushdb.bootstrap.ConfigManager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * The {@code JournalManager} is responsible for managing the operations related to
 * a journal file which stores serialized journal entries. It provides functionalities
 * to append entries, read all entries, and clear the journal.
 */
public class JournalManager {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(JournalManager.class);

    private static JournalManager instance;

    private static Properties properties;

    /**
     * Path to journal file.
     */
    private final Path journal;

    /**
     * Utilizes {@link ConfigManager} to get the journal file.
     */
    private JournalManager() {
        this.journal = Paths.get(ConfigManager.JOURNAL_FILE);
    }

    public static void reset() {
        instance = null;
    }

    public static synchronized JournalManager getInstance(Properties props) {
        if (instance == null) {
            instance = new JournalManager();
            properties = props;
        }
        return instance;
    }

    /**
     * Appends a serialized journal entry to the journal file. This method ensures that
     * the specified {@link JournalEntry} is serialized and written as a new line to the journal file.
     * Synchronized for thread safety.
     *
     * @param journalEntry the {@link JournalEntry} object to be appended to the journal file.
     *
     * @throws RuntimeException if an I/O error occurs while attempting to write to the journal file.
     */
    public synchronized void append(JournalEntry journalEntry) throws RuntimeException {
        try (BufferedWriter writer = Files.newBufferedWriter(this.journal, StandardOpenOption.APPEND)) {
            writer.write(serialize(journalEntry));
            writer.newLine();
        } catch (IOException e) {
            LOGGER.error(String.format("Failed to append journal entry: %s", journalEntry.toString()), IOException.class.getName());
            throw new RuntimeException("Failed to append journal entry: ", e);
        }
    }

    /**
     * Reads all journal entries from the associated journal file and deserializes them into a list.
     *
     * @return a list of {@link JournalEntry} objects deserialized from the journal file.
     *
     * @throws RuntimeException if an I/O error occurs while reading the journal file.
     */
    public List<JournalEntry> readAll() throws RuntimeException {
        List<JournalEntry> entries = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(this.journal)) {
            String line;
            while (( line = reader.readLine()) != null ) {
                // deserialize the line into a JournalEntry
                entries.add(deserialize(line));
            }
        } catch (IOException e) {
            LOGGER.error(String.format("Failed to read journal entries: %s", e), IOException.class.getName());
            throw new RuntimeException("Failed to read journal entries: ", e);
        }
        return entries;
    }

    /**
     * Clears the journal file by deleting it if it exists and then creating a new, empty file.
     * This method is used to reset the journal to an empty state.
     *
     * @throws RuntimeException if an I/O error occurs while clearing the journal file.
     */
    public void clear() {
        try {
            Files.deleteIfExists(this.journal);
            Files.createFile(this.journal);
        } catch (IOException e) {
            LOGGER.error(String.format("Failed to clear journal file: %s", e), IOException.class.getName());
            throw new RuntimeException("Failed to clear journal file.", e);
        }
    }

    /**
     * Serializes a {@link JournalEntry} object into a string format, combining its operation type,
     * crate name, and document ID, separated by pipe ('|') characters.
     * {@code timestamp|WRITE|Vehicle|987654321}
     * TODO: encryption?
     *
     * @param entry the {@link JournalEntry} object to be serialized
     *
     * @return a string representation of the journal entry in the format
     * {@code timestamp|operationType|crateName|documentId}
     */
    private String serialize(JournalEntry entry) {
        return entry.getTimestamp() + "|"
                + entry.getOperationType() + "|"
                + entry.getCrateName() + "|"
                + entry.getDocumentId();
    }

    /**
     * Deserializes a string representation of a journal entry into a {@link JournalEntry} object.
     * The input string is expected to have a format where fields are separated by pipe ('|')
     * characters, such as "OPERATION_TYPE|crateName|documentId".
     *
     * @param line the string representation of a journal entry, with fields separated by pipe ('|')
     *             characters. It is expected to contain the operation type, crate name, and document ID.
     *
     * @return a {@link JournalEntry} object constructed from the deserialized data.
     */
    private JournalEntry deserialize(String line) {
        String[] parts = line.split("\\|", 4);
        long timestamp = Long.parseLong(String.valueOf(parts[0]));
        JournalEntry.OperationType operationType = JournalEntry.OperationType.valueOf(parts[1]);
        String crateName = parts[2];
        long documentId = Long.parseLong(String.valueOf(parts[3]));
        return new JournalEntry(timestamp, operationType, crateName, documentId);
    }

    public Properties getProperties() {
        return properties;
    }
}
