package com.luka.simpledb.metadataManagement.exceptions;

public class IndexTableIncorrectException extends RuntimeException {
    public IndexTableIncorrectException(String message) {
        super(message);
    }

    public IndexTableIncorrectException() {
        super();
    }
}
