package com.luka.lbdb.records.exceptions;

public class FieldDuplicateNameException extends RuntimeException {
    public FieldDuplicateNameException(String message) {
        super(message);
    }

    public FieldDuplicateNameException() {
        super();
    }
}
