package com.luka.simpledb.metadataManagement.exceptions;

public class IndexAlreadyExistsException extends RuntimeException {
    public IndexAlreadyExistsException(String message) {
        super(message);
    }

    public IndexAlreadyExistsException() {
        super();
    }
}
