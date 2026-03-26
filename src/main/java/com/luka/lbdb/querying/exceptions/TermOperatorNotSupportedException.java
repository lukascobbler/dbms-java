package com.luka.lbdb.querying.exceptions;

public class TermOperatorNotSupportedException extends RuntimeException {
    public TermOperatorNotSupportedException(String message) {
        super(message);
    }

    public TermOperatorNotSupportedException() {
        super();
    }
}
