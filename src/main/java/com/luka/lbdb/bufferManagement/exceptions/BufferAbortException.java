package com.luka.lbdb.bufferManagement.exceptions;

public class BufferAbortException extends RuntimeException {
    public BufferAbortException(String message) {
        super(message);
    }

    public BufferAbortException() {
        super();
    }
}
