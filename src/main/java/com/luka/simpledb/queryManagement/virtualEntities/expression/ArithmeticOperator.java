package com.luka.simpledb.queryManagement.virtualEntities.expression;

/// Different arithmetic operators supported by the database.
public enum ArithmeticOperator {
    ADD("+"), SUB("-"), MUL("*"), DIV("/"),
    POWER("^");

    private final String symbol;
    ArithmeticOperator(String symbol) { this.symbol = symbol; }
    @Override public String toString() {
        return symbol;
    }
}
