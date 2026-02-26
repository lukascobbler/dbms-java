package com.luka.simpledb.recordManagement.exceptions;

public class FieldAlreadyAddedException extends RuntimeException {
    public FieldAlreadyAddedException(String message) {
        super(message);
    }

    public FieldAlreadyAddedException() {
        super();
    }
}
