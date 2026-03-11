package com.luka.simpledb.parsingManagement.exceptions;

public class ParserException extends RuntimeException {
    public ParserException(String message) {
        super(message);
    }

    public ParserException() {
        super();
    }
}
