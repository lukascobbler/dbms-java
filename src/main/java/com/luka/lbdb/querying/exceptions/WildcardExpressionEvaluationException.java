package com.luka.lbdb.querying.exceptions;

public class WildcardExpressionEvaluationException extends RuntimeException {
    public WildcardExpressionEvaluationException(String message) {
        super(message);
    }

    public WildcardExpressionEvaluationException() {
        super();
    }
}
