package com.luka.simpledb.transactionManagement.recoveryManagement.exceptions;

public class LogRecordParseException extends RuntimeException {
    public LogRecordParseException(String message) {
        super(message);
    }

    public LogRecordParseException() {
        super();
    }
}
