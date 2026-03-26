package com.luka.lbdb.querying.exceptions;

public class FieldRenameException extends RuntimeException {
    public FieldRenameException(String message) {
        super(message);
    }

    public FieldRenameException() {
        super();
    }
}
