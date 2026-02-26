package com.luka.simpledb.metadataManagement.exceptions;

public class IndexNotImplementedException extends RuntimeException {
    public IndexNotImplementedException(String message) {
        super(message);
    }

    public IndexNotImplementedException() {
        super();
    }
}
