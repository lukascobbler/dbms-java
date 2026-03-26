package com.luka.lbdb.metadataManagement.exceptions;

public class IndexAlreadyExistsException extends RuntimeException {
    public IndexAlreadyExistsException(String message) {
        super(message);
    }

    public IndexAlreadyExistsException() {
        super();
    }
}
