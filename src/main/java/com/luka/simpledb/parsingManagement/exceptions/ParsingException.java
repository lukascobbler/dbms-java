package com.luka.simpledb.parsingManagement.exceptions;

public class ParsingException extends RuntimeException {
    public ParsingException(String message) {
        super(message);
    }

    public ParsingException() {
        super();
    }
}
