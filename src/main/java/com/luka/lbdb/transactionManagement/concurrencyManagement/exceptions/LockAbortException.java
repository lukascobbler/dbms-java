package com.luka.lbdb.transactionManagement.concurrencyManagement.exceptions;

public class LockAbortException extends RuntimeException {
    public LockAbortException(String message) {
        super(message);
    }

    public LockAbortException() {
        super();
    }
}
