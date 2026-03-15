package com.luka.simpledb.planningManagement.exceptions;

public class QueryPlanForNonQueryStatementException extends RuntimeException {
    public QueryPlanForNonQueryStatementException(String message) {
        super(message);
    }

    public QueryPlanForNonQueryStatementException() {
        super();
    }
}
