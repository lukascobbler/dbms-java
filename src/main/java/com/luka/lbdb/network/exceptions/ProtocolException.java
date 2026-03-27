package com.luka.lbdb.network.exceptions;

public class ProtocolException extends RuntimeException {
    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException() {
        super();
    }
}
