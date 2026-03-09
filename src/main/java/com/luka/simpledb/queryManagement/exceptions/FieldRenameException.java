package com.luka.simpledb.queryManagement.exceptions;

public class FieldRenameException extends RuntimeException {
    public FieldRenameException(String message) {
        super(message);
    }

    public FieldRenameException() {
        super();
    }
}
