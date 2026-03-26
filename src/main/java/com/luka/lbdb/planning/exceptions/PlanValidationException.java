package com.luka.lbdb.planning.exceptions;

public class PlanValidationException extends RuntimeException {
    public PlanValidationException(String message) {
        super(message);
    }

    public PlanValidationException() {
        super();
    }
}
