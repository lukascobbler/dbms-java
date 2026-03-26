package com.luka.lbdb.records.exceptions;

public class FieldCannotBeNullException extends RuntimeException {
    public FieldCannotBeNullException(String message) {
        super(message);
    }

    public FieldCannotBeNullException() {
        super();
    }
}
