package com.luka.simpledb.queryManagement.exceptions;

public class NonNumericArithmeticCalculationException extends RuntimeException {
    public NonNumericArithmeticCalculationException(String message) {
        super(message);
    }

    public NonNumericArithmeticCalculationException() {
        super();
    }
}
