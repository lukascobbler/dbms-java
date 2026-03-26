package com.luka.lbdb.querying.exceptions;

public class NullComparisonException extends RuntimeException {
    public NullComparisonException(String message) {
        super(message);
    }

    public NullComparisonException() {
        super();
    }
}
