package com.luka.simpledb.metadataManagement.exceptions;

public class TableNotFoundException extends RuntimeException {
    public TableNotFoundException(String message) {
        super(message);
    }

    public TableNotFoundException() {
        super();
    }
}
