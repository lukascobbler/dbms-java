package com.luka.lbdb.transactions.exceptions;

public class BufferNotPinnedByThisTransactionException extends RuntimeException {
    public BufferNotPinnedByThisTransactionException(String message) {
        super(message);
    }

    public BufferNotPinnedByThisTransactionException() {
        super();
    }
}
