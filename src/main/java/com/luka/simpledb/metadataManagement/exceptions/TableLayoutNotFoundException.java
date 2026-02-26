package com.luka.simpledb.metadataManagement.exceptions;

public class TableLayoutNotFoundException extends RuntimeException {
    public TableLayoutNotFoundException(String message) {
        super(message);
    }

    public TableLayoutNotFoundException() {
        super();
    }
}
