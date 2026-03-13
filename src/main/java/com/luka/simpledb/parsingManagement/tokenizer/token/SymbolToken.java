package com.luka.simpledb.parsingManagement.tokenizer.token;

/// Group of tokens that describe different symbols supported by the
/// database. They include the delimiter tokens (",", ";", ...), comparison
/// tokens (">", "!=", ...) and arithmetic operation tokens ("+", "/").
public enum SymbolToken implements Token {
    EQUAL("="), PLUS("+"), MINUS("-"), DIVIDE("/"), COMMA(","), SEMICOLON(";"),
    LEFT_PAREN("("), RIGHT_PAREN(")"), GREATER_THAN(">"), GREATER_THAN_OR_EQUAL(">="),
    LESS_THAN("<"), LESS_THAN_OR_EQUAL("<="), NOT_EQUAL("!="), STAR("*"), CARET("^");

    private final String symbol;
    SymbolToken(String symbol) { this.symbol = symbol; }
    @Override public String toString() {
        return symbol;
    }
}