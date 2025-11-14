package com.luka.simpledb.bufferManagement.exceptions;

public class BufferAbortException extends RuntimeException {
    public BufferAbortException(String message) {
        super(message);
    }

    public BufferAbortException() {
        super();
    }
}
