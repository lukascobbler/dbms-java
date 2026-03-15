package com.luka.simpledb.planningManagement.exceptions;

public class UpdateQueryExecutionForNonUpdateQueryException extends RuntimeException {
    public UpdateQueryExecutionForNonUpdateQueryException(String message) {
        super(message);
    }

    public UpdateQueryExecutionForNonUpdateQueryException() {
        super();
    }
}
