package com.luka.simpledb.queryManagement.exceptions;

public class ZeroDivisionException extends RuntimeException {
    public ZeroDivisionException(String message) {
        super(message);
    }

    public ZeroDivisionException() {
        super();
    }
}
