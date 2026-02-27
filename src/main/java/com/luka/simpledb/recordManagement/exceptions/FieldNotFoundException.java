package com.luka.simpledb.recordManagement.exceptions;

public class FieldNotFoundException extends RuntimeException {
    public FieldNotFoundException(String message) {
        super(message);
    }

    public FieldNotFoundException() {
        super();
    }
}
