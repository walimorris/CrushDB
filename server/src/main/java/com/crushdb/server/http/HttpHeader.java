package com.crushdb.server.http;

/**
 * {@code com.crushdb.server.http.HttpHeader} is used to create header properties
 * on {@linkplain HttpRequest httpRequests} and {@linkplain HttpResponse httpResponse}
 * instances. A {@code header} must contain both key (name) and value properties.
 * Either property should not be null or empty.
 *
 * <p>{@code Note: we will also continuing updating evaluation of key (name) and
 * value properties for compliance with}
 *
 * <a href="https://tools.ietf.org/html/rfc7230#section-3.2">
 * RFC 7230 section-3.2</a>
 *
 * @author walimorris
 */
public class HttpHeader {

    /**
     * {@linkplain HttpHeader httpHeader} key. Also
     * known and name.
     */
    private final String key;

    /**
     * {@linkplain HttpHeader httpHeader} value
     */
    private final String value;

    /**
     * Creates a {@link HttpHeader} used in {@linkplain HttpRequest httpRequests}
     * and {@linkplain HttpResponse httpResponses}.
     *
     * @param key {@link String} header name
     * @param value {@link String} header value
     *
     * @throws IllegalArgumentException if either key or value are null or empty
     */
    public HttpHeader(String key, String value) throws IllegalArgumentException {
        if ((key == null || value == null) || (key.isEmpty() || value.isEmpty())) {
            throw new IllegalArgumentException("key and value must not be null");
        }
        this.key = key;
        this.value = value;
    }

    /**
     * Get {@linkplain HttpHeader httpHeader} key.
     *
     * @return {@link String}
     */
    public String getKey() {
        return key;
    }

    /**
     * Get {@linkplain HttpHeader httpHeader} value.
     *
     * @return {@link String}
     */
    public String getValue() {
        return value;
    }
}
