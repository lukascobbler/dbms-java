package com.luka.simpledb.recordManagement.exceptions;

public class DatabaseTypeNotImplementedException extends RuntimeException {
    public DatabaseTypeNotImplementedException(String message) {
        super(message);
    }

    public DatabaseTypeNotImplementedException() {
        super();
    }
}
