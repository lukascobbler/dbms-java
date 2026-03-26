package com.luka.lbdb.querying.exceptions;

public class IncompatibleConstantTypeException extends RuntimeException {
    public IncompatibleConstantTypeException(String message) {
        super(message);
    }

    public IncompatibleConstantTypeException() {
        super();
    }
}
