package com.luka.simpledb.recordManagement.exceptions;

public class FieldCannotBeNullException extends RuntimeException {
    public FieldCannotBeNullException(String message) {
        super(message);
    }

    public FieldCannotBeNullException() {
        super();
    }
}
