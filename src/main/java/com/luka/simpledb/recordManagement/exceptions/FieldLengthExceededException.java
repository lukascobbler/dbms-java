package com.luka.simpledb.recordManagement.exceptions;

public class FieldLengthExceededException extends RuntimeException {
    public FieldLengthExceededException(String message) {
        super(message);
    }

    public FieldLengthExceededException() {
        super();
    }
}
