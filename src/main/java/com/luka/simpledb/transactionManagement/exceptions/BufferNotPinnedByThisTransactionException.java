package com.luka.simpledb.transactionManagement.exceptions;

public class BufferNotPinnedByThisTransactionException extends RuntimeException {
    public BufferNotPinnedByThisTransactionException(String message) {
        super(message);
    }

    public BufferNotPinnedByThisTransactionException() {
        super();
    }
}
