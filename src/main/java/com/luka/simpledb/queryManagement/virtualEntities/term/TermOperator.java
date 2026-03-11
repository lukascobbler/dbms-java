package com.luka.simpledb.queryManagement.virtualEntities.term;

/// Different comparison operators supported by the database.
public enum TermOperator {
    EQUALS("="), NOT_EQUALS("!="),
    GREATER_THAN(">"), LESS_THAN("<"),
    GREATER_OR_EQUAL(">="), LESS_OR_EQUAL("<="),
    IS("IS");

    private final String symbol;
    TermOperator(String symbol) { this.symbol = symbol; }
    @Override public String toString() {
        return symbol;
    }
}
