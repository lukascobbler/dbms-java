package com.luka.lbdb.parsing.exceptions;

public class ParseException extends RuntimeException {
    public ParseException(String message) {
        super(message);
    }

    public ParseException() {
        super();
    }
}
