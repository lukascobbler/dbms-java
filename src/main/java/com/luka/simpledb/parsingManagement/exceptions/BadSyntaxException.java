package com.luka.simpledb.parsingManagement.exceptions;

public class BadSyntaxException extends RuntimeException {
    public BadSyntaxException(String message) {
        super(message);
    }

    public BadSyntaxException() {
        super();
    }
}
