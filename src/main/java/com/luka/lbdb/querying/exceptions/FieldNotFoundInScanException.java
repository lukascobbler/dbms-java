package com.luka.lbdb.querying.exceptions;

public class FieldNotFoundInScanException extends RuntimeException {
    public FieldNotFoundInScanException(String message) {
        super(message);
    }

    public FieldNotFoundInScanException() {
        super();
    }
}
