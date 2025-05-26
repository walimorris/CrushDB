package com.crushdb.server.http;

/**
 * The representation data associated with an HTTP message is either provided as the
 * content of the message or referred to by the message semantics and the target URI.
 * The representation data is in a format and encoding defined by the representation
 * metadata header fields. If you would like to read more about headers and the valid
 * representation of these fields see:
 * <a href="https://datatracker.ietf.org/doc/html/rfc9110#name-representation-data-and-met">
 *     Representation Data and Metadata</a>
 *
 * <p>These header names represent the options for HTTP request and response headers
 * and provide the means of passing further metadata.
 *
 * @author walimorris
 */
public enum HeaderName {
    /**
     * Indicates the {@linkplain MimeType media type} of the associated representation. The
     * indicated media type defines both the data format and how the data is intended to be
     * processed by a recipient, within the scope of the received message semantics.
     */
    CONTENT_TYPE("Content-Type"),

    /**
     * Indicates the associated representation's data length as a decimal non-negative integer
     * number of octets. When transferring a representation as content, it refers specifically
     * to the amount of data enclosed so that it can be used to delimit framing.
     */
    CONTENT_LENGTH("Content-Length"),

    /**
     *Indicates that the {@linkplain MimeType mime types} advertised in the {@code Content-Type}
     * headers should be respected and not changed. This header allows you to avoid
     * <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/MIME_types#mime_sniffing">
     *     Mime sniffing</a>
     * by specifying that the mime types are deliberately configured.
     */
    X_CONTENT_TYPE_OPTIONS("X-Content-Type-Options"),

    /**
     * Indicates what content codings have been applied to the representation, beyond  those
     * inherent in the media type, and thus what decoding mechanisms have to be applied in
     * order to obtain data in the media type referenced by the {@code Content-Type} header.
     */
    CONTENT_ENCODING("Content-Encoding"),

    /**
     * Describes the natural language(s) of the intended audience for the representation. This
     * might not be equivalent to all the languages used within the representation.
     */
    CONTENT_LANGUAGE("Content-Language"),

    /**
     * References a URI that can be used as an identifier for a specific resource corresponding
     * to the representation in this message's content. In other words, if one were to perform
     * a GET request in this URI at the time of the message's generation, when a 200 (OK)
     * response would contain the same representation that is enclosed as content in this msg.
     */
    CONTENT_LOCATION("Content-Location"),

    /**
     * Provides a timestamp indicating the date and time at which the origin server believes
     * the selected representation was last modified, as determined at the conclusion of
     * handling the request.
     */
    LAST_MODIFIED("Last-Modified"),

    /**
     * This field in a response provides the current entity tag for the selected representation,
     * as determined at the conclusion of handling the request. An opaque validator for
     * differentiating between multiple representations of the same resource, regardless of whether
     * those multiple representations are due to resource state change over time, content
     * negotiation resulting in multiple representations being valid at the same time, or both.
     */
    ETAG("ETag"),

    /**
     * Represents the date and time at which the message was originated. In HTTP responses,
     * this is typically the timestamp at which the server generated the response. It helps
     * intermediaries and clients to manage caching and detect stale responses.
     *
     * <pre>Format follows RFC 7231: "EEE, dd MMM yyyy HH:mm:ss zzz" (e.g., "Wed, 21 Oct 2015 07:28:00 GMT").</pre>
     *
     * See: <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Date">MDN - Date</a>
     */
    DATE("Date"),

    /**
     * Specifies the request headers that determine whether a cached response is reusable
     * for a subsequent request. When a response includes the Vary header, it tells caches
     * to consider those listed request headers when deciding if a cached response is valid.
     *
     * <pre>Commonly used with headers like {@code Accept-Encoding}, {@code User-Agent}, etc.</pre>
     *
     * See: <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Vary">MDN - Vary</a>
     */
    VARY("Vary");

    /**
     * Name of the HTTP header.
     */
    private final String headerName;

    /**
     * ENUM constructor.
     *
     * @param headerName {@link String} name of the HTTP header
     */
    HeaderName(String headerName) {
        this.headerName = headerName;
    }

    /**
     * Get HTTP header name.
     *
     * @return {@link String} string representation of header name
     */
    public String getHeaderName() {
        return this.headerName;
    }
}
