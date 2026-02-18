package com.luka.simpledb.recordManagement.exceptions;

public class RecordTooLongException extends RuntimeException {
    public RecordTooLongException(String message) {
        super(message);
    }

    public RecordTooLongException() {
        super();
    }
}
