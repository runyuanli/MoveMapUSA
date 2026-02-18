package com.rich.countyfilter;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Minimal local server:
 * - http://localhost:8080/              -> index.html
 * - http://localhost:8080/counties.json -> data/counties-10m.json
 * - http://localhost:8080/prices.tsv    -> data/prices.tsv
 */
public class Main {

    private static final int PORT = 8080;

    public static void main(String[] args) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", PORT), 0);

        server.createContext("/", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
                sendText(ex, 405, "Method Not Allowed");
                return;
            }
            if (!"/".equals(ex.getRequestURI().getPath())) {
                sendText(ex, 404, "Not Found");
                return;
            }
            Path html = Path.of("src/main/resources/public/index.html");
            if (!Files.exists(html)) {
                sendText(ex, 500, "Missing file: " + html.toAbsolutePath());
                return;
            }
            byte[] bytes = Files.readAllBytes(html);
            sendBytes(ex, 200, "text/html; charset=utf-8", bytes);
        });

        server.createContext("/counties.json", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
                sendText(ex, 405, "Method Not Allowed");
                return;
            }
            Path p = Path.of("data/counties-10m.json");
            if (!Files.exists(p)) {
                sendText(ex, 404,
                        "Missing data/counties-10m.json\n\nDownload it and place it here.\nSee README.txt for the link.");
                return;
            }
            byte[] bytes = Files.readAllBytes(p);
            sendBytes(ex, 200, "application/json; charset=utf-8", bytes);
        });

        server.createContext("/counties-hires.json", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
                sendText(ex, 405, "Method Not Allowed");
                return;
            }
            Path p = Path.of("data/counties-hires.geojson");
            if (!Files.exists(p)) {
                sendText(ex, 404, "Missing data/counties-hires.geojson");
                return;
            }
            byte[] bytes = Files.readAllBytes(p);
            sendBytes(ex, 200, "application/json; charset=utf-8", bytes);
        });

        server.createContext("/prices.tsv", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
                sendText(ex, 405, "Method Not Allowed");
                return;
            }
            Path p = Path.of("data/prices.tsv");
            if (!Files.exists(p)) {
                sendText(ex, 404, "Missing data/prices.tsv");
                return;
            }
            byte[] bytes = Files.readAllBytes(p);
            sendBytes(ex, 200, "text/tab-separated-values; charset=utf-8", bytes);
        });

        server.createContext("/life_expectancy.tsv", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
                sendText(ex, 405, "Method Not Allowed");
                return;
            }
            Path p = Path.of("data/life_expectancy.tsv");
            if (!Files.exists(p)) {
                sendText(ex, 404, "Missing data/life_expectancy.tsv");
                return;
            }
            byte[] bytes = Files.readAllBytes(p);
            sendBytes(ex, 200, "text/tab-separated-values; charset=utf-8", bytes);
        });

        server.createContext("/minimum_wage.tsv", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
                sendText(ex, 405, "Method Not Allowed");
                return;
            }
            Path p = Path.of("data/minimum_wage.tsv");
            if (!Files.exists(p)) {
                sendText(ex, 404, "Missing data/minimum_wage.tsv");
                return;
            }
            byte[] bytes = Files.readAllBytes(p);
            sendBytes(ex, 200, "text/tab-separated-values; charset=utf-8", bytes);
        });

        server.createContext("/homicide_rate.tsv", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
                sendText(ex, 405, "Method Not Allowed");
                return;
            }
            Path p = Path.of("data/homicide_rate.tsv");
            if (!Files.exists(p)) {
                sendText(ex, 404, "Missing data/homicide_rate.tsv");
                return;
            }
            byte[] bytes = Files.readAllBytes(p);
            sendBytes(ex, 200, "text/tab-separated-values; charset=utf-8", bytes);
        });

        server.createContext("/health", ex -> sendText(ex, 200, "ok"));

        server.setExecutor(null);
        server.start();

        System.out.println("County Price Map running:");
        System.out.println("  http://localhost:" + PORT + "/");
        System.out.println();
        System.out.println("If the map is blank, download counties-10m.json into data/ (see README.txt).");
    }

    private static void sendText(HttpExchange ex, int status, String body) throws IOException {
        sendBytes(ex, status, "text/plain; charset=utf-8", body.getBytes(StandardCharsets.UTF_8));
    }

    private static void sendBytes(HttpExchange ex, int status, String contentType, byte[] bytes) throws IOException {
        Headers h = ex.getResponseHeaders();
        h.set("Content-Type", contentType);
        h.set("Cache-Control", "no-cache");
        h.set("Access-Control-Allow-Origin", "*");

        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        } finally {
            ex.close();
        }
    }
}
