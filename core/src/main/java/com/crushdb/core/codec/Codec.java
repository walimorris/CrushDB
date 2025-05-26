package com.crushdb.core.codec;

import com.crushdb.core.model.document.Document;

/**
 * This {@code com.crushdb.core.codec.Codec} interface can be used as a contract for Codecs
 * that implement it. A Codec, in general terms, is a specification of conversion between
 * any type of object and any serialized form. In the case of {@code CrushDB}, the idea is
 * to provide a simple way for users of {@code CrushDB} the ability to convert their Java
 * class implementations to CrushDB {@linkplain Document documents}.
 *
 * @param <T> the encoder class that requires a codec
 *
 * @author walimorris
 */
public interface Codec<T> {

    /**
     * Encode a Java class object into a {@code CrushDB} {@linkplain Document document}.
     * Java objects can be represented as Documents by casting their fields onto a document.
     * {@linkplain Document Documents} will handle type conversion for types that are, at
     * the time, supported in {@code CrushDB} and will provide the convenient methods.
     *
     * <p>Objects that do not have an Id, will generate one when documents are created like:
     * <pre>
     * {@code Document document = new Document();}
     * </pre>
     *
     * Be aware: when documents are decoded, they will have an Id. If the Object it's being
     * decoded to does not have an Id field, it may cause complications if the Id field is
     * needed in computations.
     *
     * @param o the object to encode to an {@link Document}
     *
     * @return the encoded {@link Document}
     */
    Document encode(T o);

    /**
     * Decodes the given {@link Document} to this encoder class. This allows {@code CrushDB}
     * to represent documents as their object classes. Many times it's more convenient
     * to operate on Java Objects vs. plain {@linkplain Document documents}.
     *
     * @param document {@link Document} to decode
     *
     * @return the encoder class object
     */
    T decode(Document document);

    /**
     * Return the Codec Encoder class.
     *
     * @return encoder class
     */
    Class<T> getEncoderClass();
}
