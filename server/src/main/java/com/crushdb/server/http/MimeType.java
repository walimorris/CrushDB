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
 * <p>Disclaimer: in the absense of a MIME type, or when browsers believe they are incorrect,
 * browsers may perform {@code MIME sniffing} -- guessing the correct MIME type by looking at
 * the bytes of the resource. There are security concerns as some MIME types represent
 * executable content. Servers can prevent MIME sniffing by sending the {@code X-Content-Type-Options}
 * header. This header will be present in all {@code MicroServer} {@linkplain HttpResponse responses}.
 *
 * <p><a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/X-Content-Type-Options>
 *     nosniff</a></p>
 *
 * @author walimorris
 */
public enum MimeType {

    /**
     * All HTML content should be served with this type.
     * {@code NOTE: XHTML is mostly useless these days}.
     */
    HTML("html", "text/html"),

    /**
     * JavaScript content should always be served using this MIME Type. No other
     * MIME types are considered valid for JavaScript. This is according to:
     * <a href="https://www.iana.org/assignments/media-types/media-types.xhtml#text">
     *     MediaTypes</a>
     */
    JS("js", "application/javascript"),

    /**
     * CSS files used to style a Web page.
     */
    CSS("css", "text/css"),

    /**
     * A standard text-based format for representing structured data based on
     * JavaScript object syntax. Commonly used for transmitting data in web
     * applications.
     */
    JSON("json", "application/json"),

    /**
     * PNG images.
     */
    PNG("png", "image/png"),

    /**
     * Icon images.
     */
    ICO("ico", "image/x-icon"),

    /**
     * SVG images.
     */
    SVG("svg", "image/svg+xml"),

    /**
     * This is the default for binary files. As it means unknown binary file,
     * browsers usually don't execute it, or even as if it should be executed.
     */
    BINARY("bin", "application/octet-stream"),

    FORM("form", "application/x-www-form-urlencoded");

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
