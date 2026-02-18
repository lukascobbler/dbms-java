package com.luka.simpledb.recordManagement.exceptions;

public class FieldMissingException extends RuntimeException {
    public FieldMissingException(String message) {
        super(message);
    }

    public FieldMissingException() {
        super();
    }
}
