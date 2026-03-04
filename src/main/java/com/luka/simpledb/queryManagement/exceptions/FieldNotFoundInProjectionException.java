package com.luka.simpledb.queryManagement.exceptions;

public class FieldNotFoundInProjectionException extends RuntimeException {
    public FieldNotFoundInProjectionException(String message) {
        super(message);
    }

    public FieldNotFoundInProjectionException() {
        super();
    }
}
