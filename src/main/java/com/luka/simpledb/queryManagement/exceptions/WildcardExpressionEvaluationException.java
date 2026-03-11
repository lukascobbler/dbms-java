package com.luka.simpledb.queryManagement.exceptions;

public class WildcardExpressionEvaluationException extends RuntimeException {
    public WildcardExpressionEvaluationException(String message) {
        super(message);
    }

    public WildcardExpressionEvaluationException() {
        super();
    }
}
