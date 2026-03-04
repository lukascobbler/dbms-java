package com.luka.simpledb.queryManagement.exceptions;

public class IncompatibleConstantTypeException extends RuntimeException {
    public IncompatibleConstantTypeException(String message) {
        super(message);
    }

    public IncompatibleConstantTypeException() {
        super();
    }
}
