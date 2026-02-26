package com.luka.simpledb.metadataManagement.exceptions;

public class ViewDefinitionNotFoundException extends RuntimeException {
    public ViewDefinitionNotFoundException(String message) {
        super(message);
    }

    public ViewDefinitionNotFoundException() {
        super();
    }
}
