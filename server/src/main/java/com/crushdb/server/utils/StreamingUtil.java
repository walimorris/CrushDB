package com.crushdb.server.utils;

import java.io.IOException;
import java.io.InputStream;

import com.crushdb.server.MicroWebServer;

/**
 * <p> {@code StreamingUtil} acts as a convenient utility that helps across {@link MicroWebServer} processes
 * that have the need to work on {@link InputStream} objects in safe environments.
 *
 * <p>The following is an example of reading all bytes from an {@link InputStream}. Following this call,
 * the given input stream can be safely read/ manipulated again due to marking and resetting.
 * <pre>
 *     {@code byte[] content = StreamingUtil.readAndMarkReset(inputstream);}
 * </pre>
 *
 * @author Wali Morris
 */
public class StreamingUtil {

    /**
     * static - no constructor
     */
    private StreamingUtil() {}

    /**
     * Determines if the given {@code InputStream} supports {@link InputStream#mark(int)}.
     * If so, the input stream can be safely processed without exhausting the stream and
     * safely reused.
     * <p>
     * In this case the input stream is requested to mark the current position (the start)
     * and not read more than the limit, MAX_VALUE (effectively everything) in the steam,
     * and at some point (after the content is read) it'll reset. All bytes in the input
     * stream are read: {@link InputStream#readAllBytes()}. After, the stream is reset
     * to the previously marked position, typically the beginning of the stream.
     *
     * @param inputStream {@link InputStream} to read from and reset
     *
     * @return byte[] the full content of the input stream as a byte array
     * @throws IOException if the steam does not support mark/reset
     */
    public static byte[] readMarkAndReset(InputStream inputStream) throws IOException {
        if (!inputStream.markSupported()) {
            // this will break the server - what should it do if it can't mark and reset bozo
            throw new IOException("mark is unsupported: " + inputStream.getClass().getName());
        }
        inputStream.mark(Integer.MAX_VALUE);
        byte[] content = inputStream.readAllBytes();
        inputStream.reset();
        return content;
    }
}
