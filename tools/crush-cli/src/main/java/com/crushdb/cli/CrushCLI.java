package com.crushdb.cli;

import com.crushdb.DatabaseInitializer;
import com.crushdb.index.btree.SortOrder;
import com.crushdb.model.document.BsonType;
import com.crushdb.queryengine.QueryEngine;
import com.crushdb.storageengine.StorageEngine;

import java.util.Scanner;

public class CrushCLI {
    private final StorageEngine storageEngine;
    private final QueryEngine queryEngine;

    public CrushCLI() {
        DatabaseInitializer.init();
        storageEngine = DatabaseInitializer.getStorageEngine();
        queryEngine = DatabaseInitializer.getQueryEngine();
    }

    public void start() {
        printBanner();
        System.out.println("Welcome to CrushCLI (Type 'exit' to quit)");
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine().trim();
            if (input.equalsIgnoreCase("exit")) {
                System.out.println("Goodbye!");
                break;
            }

            try {
                handleInput(input);
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }

    private void printBanner() {
        System.out.println(" :::===== :::====  :::  === :::===  :::  === :::====  :::==== ");
        System.out.println(" :::      :::  === :::  === :::     :::  === :::  === :::  ===");
        System.out.println(" ===      =======  ===  ===  =====  ======== ===  === =======" );
        System.out.println(" ===      === ===  ===  ===     === ===  === ===  === ===  ===");
        System.out.println("  ======= ===  ===  ======  ======  ===  === =======  =======" );
        System.out.println("**************************************************************");
        System.out.println("=================**:: Welcome to CRUSHDB:: **=================");
        System.out.println();
    }

    private void handleInput(String input) {
        String[] parts = input.split(" ");

        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid command. At least <command> <crate> required.");
        }

        String command = parts[0].toLowerCase();

        switch (command) {
            case "createcrate" -> {
                if (parts.length == 2) {
                    String crateName = parts[1];
                    handleCreateCrate(crateName);
                }
            }
            case "createindex" -> {
                if (parts.length < 7) {
                    throw new IllegalArgumentException("Invalid createIndex format. Use: createIndex <crate> <bsonType> <indexName> <fieldName> <unique> <order>");
                }
                handleCreateIndex(parts[1], parts[2], parts[3], parts[4], parts[5], parts[6]);
            }
            default -> throw new IllegalArgumentException("Unknown command: " + command);
        }
    }

    private void handleCreateCrate(String crateName) {
        queryEngine.getCrateManager().createCrate(crateName);
        System.out.println("Created crate: " + crateName);
    }

    private void handleCreateIndex(String crateName, String bsonTypeStr, String indexName, String fieldName, String uniqueStr, String orderStr) {
        BsonType bsonType = BsonType.valueOf(bsonTypeStr.toUpperCase());
        boolean unique = Boolean.parseBoolean(uniqueStr);
        int order = Integer.parseInt(orderStr);
        SortOrder sortOrder = SortOrder.ASC;
        if (orderStr.equalsIgnoreCase("DESC")) {
            sortOrder = SortOrder.DESC;
        }

        queryEngine.getCrateManager().getCrate(crateName).createIndex(bsonType, indexName, fieldName, unique, order, sortOrder);
        System.out.println("Created index: " + indexName + " on crate: " + crateName);
    }

    public static void main(String[] args) {
        new CrushCLI().start();
    }
}
