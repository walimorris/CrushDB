package com.crushdb.ui;

import com.crushdb.bootstrap.CrushContext;
import com.crushdb.bootstrap.DatabaseInitializer;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class MicroServer {
    private static CrushContext cxt;

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
                if (path.equals("/")) {
                    path = "/web/index.html";
                } else if (!path.startsWith("/web/")) {
                    path = "/web" + path;
                }
                try (InputStream inputStream = MicroServer.class.getResourceAsStream(path)) {
                    System.out.println(MicroServer.class.getResource(path));
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
            String headers = "HTTP/1.1 200 OK\r\n" +
                            "Content-Type: " + contentType + "\r\n" +
                            "Content-Length: " + content.length + "\r\n" +
                            "\r\n";
            outputStream.write(headers.getBytes(StandardCharsets.UTF_8));
            outputStream.write(content);
        } catch (IOException e) {
            System.out.println("Error: serving resource: " + e.getMessage());
        }
    }

    private static void pageNotFound(OutputStream out) throws IOException {
        String body = "<h1>404 Not Found</h1>";
        out.write((
                "HTTP/1.1 404 Not Found\r\n" +
                "Content-Type: text/html\r\n" +
                "Content-Length: " + body.length() + "\r\n" +
                "\r\n" + body).getBytes()
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
