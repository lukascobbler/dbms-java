package com.luka.lbdb.metadataManagement.exceptions;

public class TableDuplicateNameException extends RuntimeException {
    public TableDuplicateNameException(String message) {
        super(message);
    }

    public TableDuplicateNameException() {
        super();
    }
}
