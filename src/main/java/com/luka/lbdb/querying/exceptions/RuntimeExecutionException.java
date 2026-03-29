package com.luka.lbdb.querying.exceptions;

public class RuntimeExecutionException extends RuntimeException {
    public RuntimeExecutionException(String message) {
        super(message);
    }

    public RuntimeExecutionException() {
        super();
    }
}
