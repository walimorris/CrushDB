package com.crushdb.server.http;

/**
 * {@code com.crushdb.server.http.HttpHeader} is used to create header properties
 * on {@linkplain HttpRequest httpRequests} and {@linkplain HttpResponse httpResponse}
 * instances. A {@code header} must contain both header name and value properties.
 * Either property should not be null or empty.
 *
 * <p>{@code Note: we will also continuing updating evaluation of header name and
 * value properties for compliance with}
 *
 * <a href="https://tools.ietf.org/html/rfc7230#section-3.2">
 * RFC 7230 section-3.2</a>
 *
 * @author walimorris
 */
public class HttpHeader {

    /**
     * {@linkplain HttpHeader httpHeader} name.
     */
    private final String headerName;

    /**
     * {@linkplain HttpHeader httpHeader} value
     */
    private final String value;

    /**
     * Creates a {@link HttpHeader} used in {@linkplain HttpRequest httpRequests}
     * and {@linkplain HttpResponse httpResponses}.
     *
     * @param headerName {@link String} header name
     * @param value {@link String} header value
     *
     * @throws IllegalArgumentException if either header name or value are null or empty
     */
    public HttpHeader(String headerName, String value) throws IllegalArgumentException {
        if ((headerName == null || value == null) || (headerName.isEmpty() || value.isEmpty())) {
            throw new IllegalArgumentException("header name and value must not be null");
        }
        this.headerName = headerName;
        this.value = value;
    }

    /**
     * Get {@linkplain HttpHeader httpHeader} name.
     *
     * @return {@link String}
     */
    public String getHeaderName() {
        return headerName;
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
