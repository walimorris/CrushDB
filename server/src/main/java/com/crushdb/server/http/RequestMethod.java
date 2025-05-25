package com.crushdb.server.http;

/**
 * {@code com.crushdb.server.http.RequestMethod} is a convenient ENUM class utilized
 * in {@code MicroServer}'s {@linkplain HttpRequest httpRequests}. Request methods
 * indicate the purpose of the request and what is expected if the request is
 * successful.
 *
 * <p>You can read more into request methods here:
 * <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Methods">
 *     HTTP request Methods</a>
 *
 * @author walimorris
 */
public enum RequestMethod {

    /**
     * Requests a representation of the specified resource. GET should
     * only retrieve data and should not contain a request content.
     */
    GET("GET"),

    /**
     * Submits an entity to the specified resource, often causes a change in state
     * or side effects on the server.
     */
    PUT("PUT"),

    /**
     * Asks for a response identical to a GET request, but without a response body.
     */
    HEAD("HEAD"),

    /**
     * Establishes a tunnel to the server identified by the target resource.
     */
    CONNECT("CONNECT"),

    /**
     * Describes the communication options for the target resource.
     */
    OPTIONS("OPTIONS"),

    /**
     * Performs a message loop-back test along the path to the target resource.
     */
    TRACE("TRACE"),

    /**
     * Applies partial modifications to a resource.
     */
    PATCH("PATCH"),

    /**
     * Submits an entity to the specified resource, often causing a change
     * in state or side effects on the server.
     */
    POST("POST"),

    /**
     * Deletes the specified resource.
     */
    DELETE("DELETE");

    /**
     * The {@linkplain RequestMethod requestMethod} request type.
     */
    private final String type;

    /**
     * Creates a request with the given type.
     *
     * @param type {@link String} string representation of the ENUM
     *                           request method type
     */
    RequestMethod(String type) {
        this.type = type;
    }

    /**
     * Get this request method's string representation.
     *
     * @return {@link String} string representation of this
     * {@link RequestMethod}
     */
    public String getType() {
        return type;
    }
}
