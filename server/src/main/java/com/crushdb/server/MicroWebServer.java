package com.crushdb.server;

import com.crushdb.core.bootstrap.CrushContext;
import com.crushdb.core.bootstrap.DatabaseInitializer;
import com.crushdb.core.providers.StaticResourceProvider;
import com.crushdb.server.config.RouterConfig;
import com.crushdb.server.http.*;
import com.crushdb.server.handler.RouteHandler;
import com.crushdb.server.router.Router;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.crushdb.server.http.MimeType.fromPath;

public class MicroWebServer {
    private static CrushContext cxt;
    private static final String NO_FOUND_STATIC_RESOURCE_PROVIDER = "No StaticResourceProvider found";

    private static final String RN = "\r\n";
    private static final String HTTP_V1_404 = "HTTP/1.1 404 Not Found";
    private static final String HTTP_V1_302 = "Http/1.1 302 Found";
    private static final String CONTENT_TYPE = "Content-Type: ";
    private static final String CONTENT_LENGTH = "Content-Length: ";
    private static final String INDEX = "/";
    private static final String INDEX_HTML = "index.html";
    private static final String WEB_PREFIX = "/web";

    public static void main(String[] args) throws IOException {
        cxt = DatabaseInitializer.init();
        int port = cxt.getPort();
        Router router = RouterConfig.init();
        ExecutorService executor = Executors.newFixedThreadPool(5);
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("CrushDB Microserver running at http://localhost:" + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(() -> {
                    System.out.print("Processed by thread: " + Thread.currentThread());
                    handleClient(clientSocket, cxt, router);
                });
            }
        } finally {
            executor.shutdown();
        }
    }

    private static void handleClient(Socket socket, CrushContext cxt, Router router) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            OutputStream outputStream = socket.getOutputStream()) {
            String line = in.readLine();
            if (line != null) {
                System.out.println("Request=" + line);
                String[] browserRequest = line.split(" ");
                String method = browserRequest[0];
                String path = browserRequest[1];
                HttpVersion requestVersion = HttpVersion.version(browserRequest[2]);

                // IoC usage to get the web resources for markup, js, css
                path = resolve(path);
                System.out.println("resolved path: " + path);
                ServiceLoader<StaticResourceProvider> loader = ServiceLoader.load(StaticResourceProvider.class);
                StaticResourceProvider provider = loader.findFirst()
                        .orElseThrow(() -> new IllegalStateException(NO_FOUND_STATIC_RESOURCE_PROVIDER));
                System.out.println("resource provider = " + provider.getClass().getName());
                try (InputStream inputStream = provider.getResource(path)) {
                    if (inputStream == null) {
                        pageNotFound(outputStream);
                    } else {
                        MimeType mimeType = fromPath(path);
                        RequestMethod requestMethod = RequestMethod.valueOf(method);
                        // TODO: make resolve(request)
                        // there are many resources in /web we can let the page handler
                        // handle these they're all the same (write headers, write stream)
                        RouteHandler routeHandler = router.resolveCommonRoute(requestMethod, path);
                        if (routeHandler == null) {
                            System.out.println("No route handler found for path: " + path);
                            pageNotFound(outputStream);
                            return;
                        }
                        if (cxt.getAuthenticator() == null) {
                            System.out.println("Authenticator is null.");
                            redirectToSignIn(outputStream);
                            return;
                        } else {
                            boolean isAuthenticated = cxt.getAuthenticator().isAuthenticated();
                            if (!isAuthenticated) {
                                redirectToSignIn(outputStream);
                                return;
                            }
                        }
                        HttpRequest httpRequest = HttpRequest.newBuilder(path)
                                .method(requestMethod)
                                .version(requestVersion)
                                .mimetype(mimeType)
                                .inputStream(inputStream)
                                .build();
                        HttpResponse response = new HttpResponse();
                        routeHandler.handle(httpRequest, response, cxt);
                        write(response, outputStream);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String resolve(String path) {
        String resolvedPath = "";
        if (path.startsWith("/api")) {
            resolvedPath = path;
        } else if (path.equals(INDEX)) {
            resolvedPath = WEB_PREFIX + INDEX + INDEX_HTML;
        } else {
            if (!path.startsWith(WEB_PREFIX)) {
                resolvedPath = WEB_PREFIX + path;
            }
        }
        return resolvedPath;
    }

    private static void write(HttpResponse response, OutputStream outputStream) {
        try {
            String responseCapture = captureHeaderResponse(response);
            if (responseCapture != null) {
                System.out.println("headers:");
                System.out.println(responseCapture);
            }
            outputStream.write(response.byteHeaders());
            System.out.println("Wrote headers");
            outputStream.write(response.body());
            System.out.println("Wrote body");
        } catch (IOException e) {
            System.out.println("Error: serving resource: " + e.getMessage());
        }
    }

    private static String captureHeaderResponse(HttpResponse response) {
        byte[] responseHeaderBytes = response.byteHeaders();
        try (InputStream stream = new ByteArrayInputStream(responseHeaderBytes);
             InputStreamReader inputStreamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(inputStreamReader)) {
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }
            return builder.toString();
        } catch (IOException e) {
            System.out.println("Can't read responseHeaderBytes input stream");
        }
        return null;
    }

    private static void pageNotFound(OutputStream out) throws IOException {
        String body = "<h1>404 Not Found</h1>";
        out.write((HTTP_V1_404 + RN + CONTENT_TYPE + "text/html" + RN +
                CONTENT_LENGTH + body.length() + RN + RN + body).getBytes()
        );
    }

    private static void redirectToSignIn(OutputStream out) throws IOException {
        String body = "<html><body>Redirecting to <a href=\"/signin.html\">signin</a>...</body></html>";
        String headers = "HTTP/1.1 302 Found\r\n" +
                "Location: /signin.html\r\n" +
                "Content-Type: text/html\r\n" +
                "Content-Length: " + body.getBytes(StandardCharsets.UTF_8).length + "\r\n" +
                "\r\n";

        out.write(headers.getBytes(StandardCharsets.UTF_8));
        out.write(body.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
