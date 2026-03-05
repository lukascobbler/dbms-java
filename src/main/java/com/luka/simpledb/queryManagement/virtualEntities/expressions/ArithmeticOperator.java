package com.luka.simpledb.queryManagement.virtualEntities.expressions;

public enum ArithmeticOperator {
    ADD("+"), SUB("-"), MUL("*"), DIV("/");

    private final String symbol;
    ArithmeticOperator(String symbol) { this.symbol = symbol; }
    @Override public String toString() {
        return symbol;
    }
}
