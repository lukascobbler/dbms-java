package com.luka.lbdb.metadataManagement.exceptions;

public class IndexDuplicateNameException extends RuntimeException {
    public IndexDuplicateNameException(String message) {
        super(message);
    }

    public IndexDuplicateNameException() {
        super();
    }
}
