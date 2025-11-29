package com.luka.simpledb.transactionManagement.concurrencyManagement.exceptions;

public class LockAbortException extends RuntimeException {
    public LockAbortException(String message) {
        super(message);
    }

    public LockAbortException() {
        super();
    }
}
