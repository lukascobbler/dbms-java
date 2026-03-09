package com.luka.simpledb.queryManagement.exceptions;

public class NullComparisonException extends RuntimeException {
    public NullComparisonException(String message) {
        super(message);
    }

    public NullComparisonException() {
        super();
    }
}
