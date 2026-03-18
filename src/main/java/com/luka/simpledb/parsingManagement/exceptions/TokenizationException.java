package com.luka.simpledb.parsingManagement.exceptions;

public class TokenizationException extends ParseException {
    public TokenizationException(String message) {
        super(message);
    }

    public TokenizationException() {
        super();
    }
}
