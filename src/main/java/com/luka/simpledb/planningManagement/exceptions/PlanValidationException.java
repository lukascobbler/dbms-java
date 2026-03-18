package com.luka.simpledb.planningManagement.exceptions;

public class PlanValidationException extends RuntimeException {
    public PlanValidationException(String message) {
        super(message);
    }

    public PlanValidationException() {
        super();
    }
}
