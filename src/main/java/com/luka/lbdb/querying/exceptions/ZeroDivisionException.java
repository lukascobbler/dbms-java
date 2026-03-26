package com.luka.lbdb.querying.exceptions;

public class ZeroDivisionException extends RuntimeException {
    public ZeroDivisionException(String message) {
        super(message);
    }

    public ZeroDivisionException() {
        super();
    }
}
