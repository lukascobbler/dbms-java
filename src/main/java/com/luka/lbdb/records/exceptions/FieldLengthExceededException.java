package com.luka.lbdb.records.exceptions;

public class FieldLengthExceededException extends RuntimeException {
    public FieldLengthExceededException(String message) {
        super(message);
    }

    public FieldLengthExceededException() {
        super();
    }
}
