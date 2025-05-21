package com.crushdb.server;

import com.crushdb.core.bootstrap.CrushContext;
import com.crushdb.core.bootstrap.DatabaseInitializer;
import com.crushdb.core.providers.StaticResourceProvider;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ServiceLoader;

public class MicroWebServer {
    private static CrushContext cxt;

    private static final String INDEX = "/";
    private static final String INDEX_HTML = "index.html";
    private static final String WEB_PREFIX = "/web";
    private static final String NO_FOUND_STATIC_RESOURCE_PROVIDER = "No StaticResourceProvider found";

    private static final String RN = "\r\n";
    private static final String HTTP_V1_200 = "HTTP/1.1 200 OK";
    private static final String HTTP_V1_404 = "HTTP/1.1 404 Not Found";
    private static final String CONTENT_TYPE = "Content-Type: ";
    private static final String CONTENT_LENGTH = "Content-Length: ";

    public static void main(String[] args) throws IOException {
        cxt = DatabaseInitializer.init();
        int port = cxt.getPort();
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("CrushDB Microserver running at http://localhost:" + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket, cxt)).start();
            }
        }
    }

    private static void handleClient(Socket socket, CrushContext cxt) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            OutputStream outputStream = socket.getOutputStream()) {
            String line = in.readLine();
            if (line != null) {
                System.out.println("Request=" + line);
                String[] request = line.split(" ");
                String method = request[0];
                String path = request[1];

                if (path.equals(INDEX)) {
                    path = WEB_PREFIX + INDEX + INDEX_HTML;
                } else if (!path.startsWith(WEB_PREFIX + INDEX)) {
                    path = WEB_PREFIX + path;
                }
                ServiceLoader<StaticResourceProvider> loader = ServiceLoader.load(StaticResourceProvider.class);
                StaticResourceProvider provider = loader.findFirst()
                        .orElseThrow(() -> new IllegalStateException(NO_FOUND_STATIC_RESOURCE_PROVIDER));

                try (InputStream inputStream = provider.getResource(path)) {
                    if (inputStream == null) {
                        pageNotFound(outputStream);
                    } else {
                        loadResource(inputStream, outputStream, getMimeType(path));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void loadResource(InputStream inputStream, OutputStream outputStream, String contentType) {
        try {
            byte[] content = inputStream.readAllBytes();
            String headers = HTTP_V1_200 + RN + CONTENT_TYPE + contentType + RN +
                    CONTENT_LENGTH + content.length + RN + RN;
            outputStream.write(headers.getBytes(StandardCharsets.UTF_8));
            outputStream.write(content);
        } catch (IOException e) {
            System.out.println("Error: serving resource: " + e.getMessage());
        }
    }

    private static void pageNotFound(OutputStream out) throws IOException {
        String body = "<h1>404 Not Found</h1>";
        out.write((HTTP_V1_404 + RN + CONTENT_TYPE + "text/html" + RN +
                CONTENT_LENGTH + body.length() + RN + RN + body).getBytes()
        );
    }

    private static String getMimeType(String path) {
        String[] pathSplit = path.split("\\.");
        String mimeType = pathSplit[pathSplit.length - 1];

        return switch (mimeType) {
            case "html" -> "text/html";
            case "js" -> "application/javascript";
            case "css" -> "text/css";
            case "json" -> "application/json";
            case "png" -> "image/png";
            case "ico" -> "image/x-icon";
            case "svg" -> "image/svg+xml";
            default -> "application/octet-stream";
        };
    }
}
