package com.crushdb.server.handler;

import com.crushdb.core.authentication.Authenticator;
import com.crushdb.core.bootstrap.CrushContext;
import com.crushdb.server.http.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class AuthenticationHandler implements RouteHandler {

    @Override
    public void handle(HttpRequest request, HttpResponse response, CrushContext cxt) {
        response.setImmutableHttpRequest((ImmutableHttpRequest) request);
        response.setVersion(request.version());

        MimeType contentType = null;
        for (HttpHeader header : request.headers().set()) {
            if (header.getHeaderName().equals(HeaderName.CONTENT_TYPE.name())) {
                contentType = MimeType.valueOf(header.getValue());
            }
        }
        if (contentType != null) {
            if (!cxt.getAuthenticator().isAuthenticated() && contentType.equals(MimeType.FORM)) {
                // get the form data
                HttpHeaders headers = new HttpHeaders();
                Map<String, String> formData = readForm(request.inputStream());
                if (formData != null) {
                    Authenticator webAuthenticator = cxt.getAuthenticator();
                    if (webAuthenticator.authenticate(formData.get("username"), formData.get("password"))) {
                        System.out.println("user authenticated");
                        HttpHeader redirectHeader = new HttpHeader("Location", "/");
                        headers.addHeader(redirectHeader);
                        response.setHeaders(headers);
                        response.setStatusCode(302);
                    } else {
                        System.out.println("user not authenticated");
                        response.setStatusCode(401);
                    }
                }
            }
        } else {
            response.setStatusCode(500);
        }
    }

    private Map<String, String> readForm(InputStream inputStream) throws IllegalArgumentException {
        Map<String, String> formData = new HashMap<>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.startsWith("username")) {
                    // example: username=admin&password=secret
                    String[] authenticationDataParts = line.split("&");
                    // [username=admin], [password=secret]
                    if (authenticationDataParts.length == 2) {
                        String[] usernameParts = authenticationDataParts[0].split("=");
                        String[] passwordParts = authenticationDataParts[1].split("=");
                        if (usernameParts.length == 2 && passwordParts.length == 2) {
                            formData.put(usernameParts[0], usernameParts[1]);
                            formData.put(passwordParts[0], passwordParts[1]);
                            return formData;
                        } else {
                            throw new IllegalStateException("authentication data is malformed");
                        }
                    } else {
                        throw new IllegalStateException("authentication data is malformed.");
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Error reading authentication request form data: " + e.getMessage());
        }
        return null;
    }
}
