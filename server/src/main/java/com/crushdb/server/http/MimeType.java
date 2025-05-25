package com.crushdb.server.http;

/**
 * If you're unfamiliar with MimeType usage in browsers, here's a great resource to quickly get
 * up to speed:
 * <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/MIME_types">MimeTypes</a>
 *
 * <p>{@code MimeType} is a convenient ENUM class to assist with processing URLS.
 * It's important that web servers send the correct MIME type in the response's <b>Content-Type</b>
 * header. Without proper configurations, browsers are likely to misinterpret the contents of files,
 * and sites will not work correctly.
 *
 * @author walimorris
 */
public enum MimeType {
    HTML("html", "text/html"),
    JS("js", "application/javascript"),
    CSS("css", "text/css"),
    JSON("json", "application/json"),
    PNG("png", "image/png"),
    ICO("ico", "image/x-icon"),
    SVG("svg", "image/svg+xml"),
    BINARY("bin", "application/octet-stream");

    /**
     * The {@code MimeType} file extension.
     */
    private final String ext;

    /**
     * The {@code MimeType} media type.
     */
    private final String type;

    /**
     * MimeType contains its extension and media type.
     *
     * @param ext the file extension
     * @param type the media type
     */
    MimeType(String ext, String type) {
        this.ext = ext;
        this.type = type;
    }

    /**
     * Get {@code MimeType} file extension.
     *
     * @return {@link String}
     */
    public String getExt() {
        return ext;
    }

    /**
     * Get {@code MimeType} media type.
     *
     * @return {@link String}
     */
    public String getType() {
        return type;
    }

    /**
     * Get {@code MimeType} subtype.
     *
     * @return {@link String}
     */
    public String getSubtype() {
        return type.split("/")[1];
    }

    /**
     * Get {@code MimeType} subtype.
     *
     * @param mimeType {@link MimeType}
     *
     * @return {@link String}
     */
    public static String getSubtype(MimeType mimeType) {
        return mimeType.type.split("/")[1];
    }

    /**
     * Given a file path, pull the file extension value,
     * run an equality matcher against all other possible
     * {@code MimeType} extensions. Return the media type
     * or else return binary octet stream type.
     *
     * @param path given file path to resource content
     *
     * @return {@link MimeType}
     */
    public static MimeType fromPath(String path) {
        if (path == null || !path.contains(".")) {
            return BINARY;
        }
        String ext = path.substring(path.lastIndexOf(".") + 1).toLowerCase();
        for (MimeType type : values()) {
            if (type.ext.equals(ext)) {
                return type;
            }
        }
        return BINARY;
    }
}
