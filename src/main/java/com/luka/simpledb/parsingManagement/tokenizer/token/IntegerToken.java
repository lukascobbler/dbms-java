package com.luka.simpledb.parsingManagement.tokenizer.token;

public record IntegerToken(int value) implements Token {
    @Override
    public String toString() { return "integer(" + value + ")"; }
}