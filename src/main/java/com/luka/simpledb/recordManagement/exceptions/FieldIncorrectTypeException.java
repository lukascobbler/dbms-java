package com.luka.simpledb.recordManagement.exceptions;

public class FieldIncorrectTypeException extends RuntimeException {
    public FieldIncorrectTypeException(String message) {
        super(message);
    }

    public FieldIncorrectTypeException() {
        super();
    }
}
