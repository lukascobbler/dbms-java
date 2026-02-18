package com.luka.simpledb.recordManagement.exceptions;

public class FieldLimitException extends RuntimeException {
    public FieldLimitException(String message) {
        super(message);
    }

    public FieldLimitException() {
        super();
    }
}
