package com.luka.simpledb.recordManagement.exceptions;

public class FieldDuplicateNameException extends RuntimeException {
    public FieldDuplicateNameException(String message) {
        super(message);
    }

    public FieldDuplicateNameException() {
        super();
    }
}
