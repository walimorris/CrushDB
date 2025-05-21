package com.crushdb.cli;

import com.crushdb.core.bootstrap.CrushContext;
import com.crushdb.core.bootstrap.DatabaseInitializer;
import com.crushdb.core.index.btree.SortOrder;
import com.crushdb.core.model.document.BsonType;
import com.crushdb.core.model.document.Document;
import com.crushdb.core.utils.FileUtil;

import java.util.List;
import java.util.Map;
import java.util.Scanner;

import java.io.IOException;

public class CrushCLI {
    private CrushContext cxt;

    private final List<String> singleCommands;

    public CrushCLI() {
        cxt = DatabaseInitializer.initTest();
        singleCommands = List.of("clear", "help");
    }

    public void start() {
        printBanner();
        System.out.println("Welcome to CrushCLI (Type 'exit' to quit)");
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine().trim();
            if (input.equalsIgnoreCase("exit")) {

                // clean up
                System.out.println("Goodbye!");
                if (cxt.isTest()) {
                    FileUtil.destroyTestDatabaseDirectory();
                }
                scanner.close();
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
        String[] parts;
        String lowered = input.toLowerCase();

        if (lowered.startsWith("insertone") || lowered.startsWith("find")) {
            parts = input.split(" ", 3);
        } else {
            parts = input.split(" ");
        }

        if (parts.length < 2 && !singleCommands.contains(parts[0])) {
            throw new IllegalArgumentException("Invalid command. At least <command> <crate> required.");
        }

        String command = parts[0].toLowerCase();
        System.out.println("COMMAND: " + command);

        switch (command) {
            case "createcrate" -> {
                if (parts.length == 2) {
                    String crateName = parts[1];
                    handleCreateCrate(crateName);
                }
            }
            case "createindex" -> {
                if (parts.length < 7) {
                    throw new IllegalArgumentException(
                            "Invalid createIndex format. Use: createIndex <crate> <bsonType> <indexName> <fieldName> <unique> <order>"
                    );
                }
                handleCreateIndex(parts[1], parts[2], parts[3], parts[4], parts[5], parts[6]);
            }
            case "insertone" -> {
                if (parts.length == 3) {
                    String crateName = parts[1];
                    String documentJson = parts[2].trim();
                    handleInsertOne(crateName, documentJson);
                }
            }
            case "find" -> {
                if (parts.length == 3) {
                    String crateName = parts[1];
                    String query = parts[2].trim();
                    handleFind(crateName, query);
                }
            }
            case "clear" -> {
                clearScreen();
                printBanner();
                System.out.println("Welcome to CrushCLI (Type 'exit' to quit)");
            }
            default -> throw new IllegalArgumentException("Unknown command: " + command);
        }
    }

    private void handleCreateCrate(String crateName) {
        cxt.getQueryEngine().getCrateManager().createCrate(crateName);
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

        cxt.getQueryEngine().getCrateManager().getCrate(crateName).createIndex(bsonType, indexName, fieldName, unique, order, sortOrder);
        System.out.println("Created index: " + indexName + " on crate: " + crateName);
    }

    private void handleInsertOne(String crateName, String json) {
        System.out.println(json);
        Document document = Document.fromJson(json);
        document.prettyPrint();
        Document resultDocument = cxt.getQueryEngine().getCrateManager().getCrate(crateName).insert(document);
        resultDocument.prettyPrint();
    }

    private void handleFind(String crateName, String json) {
        // {"vehicle_year": { "$gt": 2015 }}
        String content = json.substring(1);
        content = content.substring(0, content.length() - 1);

        // we get from example: "vehicle": { "$gt": 2015}
        // now we work on each part
        String[] parts = content.split(":", 2);
        String field = parts[0].trim().replace("\"", "");

        // now we work on operator and value
        String opAndValuePart = parts[1].trim();
        opAndValuePart = opAndValuePart.substring(1);
        opAndValuePart = opAndValuePart.substring(0, opAndValuePart.length() - 1).trim();

        // "$gt": 2015
        String[] splitsOpAndValue = opAndValuePart.split(":");
        String operator = splitsOpAndValue[0].trim().replace("\"", "");
        String value = splitsOpAndValue[1].trim();

        Object typedValue;

        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            typedValue = Boolean.parseBoolean(value);
        } else if (value.startsWith("\"") && value.endsWith("\"")) {
            // remove quotes for strings
            typedValue = value.substring(1, value.length() - 1);
        } else if (value.contains(".")) {
            // float or double
            if (value.length() < 8) {
                typedValue = Float.parseFloat(value);
            } else {
                typedValue = Double.parseDouble(value);
            }
        } else {
            // int or long
            if (value.length() >= 10) {
                typedValue = Long.parseLong(value);
            } else {
                typedValue = Integer.parseInt(value);
            }
        }
        Map<String, Object> query = Map.of(field, Map.of(operator, typedValue));
        List<Document> results = cxt.getQueryEngine().find(crateName, query);
        for (Document result : results) {
            result.prettyPrint();
        }
    }

    /**
     * Uses system properties to determine which operating system being used. The respective
     * operating system "clear" method is utilized to clear crushcli screen.
     */
    public static void clearScreen() {
        try {
            String os = System.getProperty("os.name");
            if (os.contains("Windows")) {
                new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
            } else {
                new ProcessBuilder("clear").inheritIO().start().waitFor();
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Error clearing the console: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        new CrushCLI().start();
    }
}
