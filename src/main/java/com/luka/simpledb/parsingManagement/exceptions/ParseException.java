package com.luka.simpledb.parsingManagement.exceptions;

public class ParseException extends RuntimeException {
    public ParseException(String message) {
        super(message);
    }

    public ParseException() {
        super();
    }
}
