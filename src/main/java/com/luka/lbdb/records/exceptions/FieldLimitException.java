package com.luka.lbdb.records.exceptions;

public class FieldLimitException extends RuntimeException {
    public FieldLimitException(String message) {
        super(message);
    }

    public FieldLimitException() {
        super();
    }
}
