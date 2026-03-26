package com.luka.lbdb.querying.exceptions;

public class NonNumericArithmeticCalculationException extends RuntimeException {
    public NonNumericArithmeticCalculationException(String message) {
        super(message);
    }

    public NonNumericArithmeticCalculationException() {
        super();
    }
}
