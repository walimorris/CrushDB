package com.crushdb.storageengine.page;

import com.crushdb.logger.CrushDBLogger;
import com.crushdb.storageengine.config.ConfigManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;



public class MetaFileManager {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(MetaFileManager.class);

    /**
     * Represents the file path to the metadata file used by the PageManager.
     *
     * This file is used for storing or managing metadata essential to the PageManager's
     * operations, such as page tracking, cache management, or other data.
     */
    private static final Path META_PATH = Paths.get(ConfigManager.get(ConfigManager.META_FILE_FIELD, null));

    /**
     * The CrushDB Magic Number is used as a unique identifier or signature within
     * CrushDB metadata files.
     *
     * Validates the integrity and format of the metadata file, ensuring that it
     * matches the expected structure for correct operation.
     */
    protected static final int MAGIC_NUMBER = 0x43525553;

    /**
     * Represents the version of the metadata format used by the `MetaFileManager`.
     *
     * This variable is used to ensure compatibility between the metadata file
     * and the software that processes it. Changes to this value may reflect updates
     * or modifications in the metadata structure or format.
     */
    protected static final int VERSION = 1;

    /**
     * Represents the fixed capacity allocated for the metadata buffer.
     *
     * This constant defines the byte size of the buffer used during read/write
     * operations for metadata. The value should be consistent with the structure
     * and requirements of the metadata storage.
     */
    protected static final int CAPACITY = 17;

    public static void writeMetadata(Metadata metadata) throws IllegalArgumentException {
        if (isValidMetadata(metadata)) {
            try (FileChannel channel = FileChannel.open(META_PATH, CREATE, WRITE, TRUNCATE_EXISTING)) {
                ByteBuffer buffer = ByteBuffer.allocate(CAPACITY);
                buffer.putInt(metadata.magicNumber());
                buffer.put((byte) metadata.version());
                buffer.putLong(metadata.lastPageId());
                buffer.putInt(0); // reserved
                buffer.flip();
                channel.write(buffer);
            } catch (IOException e) {
                LOGGER.error("Cannot read metadata file, ensure correct magic number and version" + metadata,
                        IOException.class.getName());
            }
        } else {
            throw new IllegalArgumentException("Cannot write metadata file, ensure correctness: " + metadata);
        }
    }

    public static Metadata readMetadata() {
        if (!Files.exists(META_PATH)) {
            // if there's no meta file - create it
            return new Metadata(MAGIC_NUMBER, VERSION, 0L);
        }
        try (FileChannel channel = FileChannel.open(META_PATH, READ)) {
            // empty meta file - it's created when database inits
            if (channel.size() == 0) {
                return new Metadata(MAGIC_NUMBER, VERSION, 0L);
            }
            ByteBuffer buffer = ByteBuffer.allocate(CAPACITY);
            channel.read(buffer);
            buffer.flip();

            int magicNumber = buffer.getInt();
            byte version = buffer.get();
            long lastPageId = buffer.getLong();
            buffer.getInt();

            return new Metadata(magicNumber, version, lastPageId);
        } catch (IOException e) {
            LOGGER.error("Error reading metadata file: " + META_PATH, IOException.class.getName());
            return null;
        }
    }

    private static boolean isValidMetadata(Metadata metadata) {
        return metadata.magicNumber() == MAGIC_NUMBER && metadata.version() == VERSION;
    }
}
