package com.luka.lbdb.records.exceptions;

public class RecordTooLongException extends RuntimeException {
    public RecordTooLongException(String message) {
        super(message);
    }

    public RecordTooLongException() {
        super();
    }
}
