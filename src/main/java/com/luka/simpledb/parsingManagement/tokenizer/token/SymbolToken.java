package com.luka.simpledb.parsingManagement.tokenizer.token;

public enum SymbolToken implements Token {
    EQUAL("="), PLUS("+"), MINUS("-"), DIVIDE("/"), COMMA(","), DOT("."), SEMICOLON(";"),
    LEFT_PAREN("("), RIGHT_PAREN(")"), GREATER_THAN(">"), GREATER_THAN_OR_EQUAL(">="),
    LESS_THAN("<"), LESS_THAN_OR_EQUAL("<="), NOT_EQUAL("!="), STAR("*");

    private final String symbol;
    SymbolToken(String symbol) { this.symbol = symbol; }
    @Override public String toString() {
        return symbol;
    }
}