package com.crushdb.ui;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;

public class MicroServer {
    public static void main(String[] args) throws IOException {
        int port = 8082;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("CrushDB Microserver running at http://localhost:" + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        }
    }

    private static void handleClient(Socket socket) {
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
                }
                try (InputStream inputStream = MicroServer.class.getResourceAsStream(path)) {
                    System.out.println(MicroServer.class.getResource(path));
                    if (inputStream == null) {
                        pageNotFound(outputStream);
                    } else {
                        loadResource(inputStream, outputStream);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void loadResource(InputStream inputStream, OutputStream outputStream) {
        try {
            byte[] content = inputStream.readAllBytes();
            String headers = "HTTP/1.1 200 OK\r\n" +
                            "Content-Type: text/html\r\n" +
                            "Content-Length: " + content.length + "\r\n" +
                            "\r\n";
            outputStream.write(headers.getBytes(StandardCharsets.UTF_8));
            outputStream.write(content);
        } catch (IOException e) {
            System.out.println("Error: reading and writing resource content\n" + e.getMessage());
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
}
