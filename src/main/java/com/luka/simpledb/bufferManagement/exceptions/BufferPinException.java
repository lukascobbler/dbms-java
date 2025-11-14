package com.luka.simpledb.bufferManagement.exceptions;

public class BufferPinException extends RuntimeException {
    public BufferPinException(String message) {
        super(message);
    }

    public BufferPinException() {
        super();
    }
}
