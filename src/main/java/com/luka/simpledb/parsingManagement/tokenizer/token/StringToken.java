package com.luka.simpledb.parsingManagement.tokenizer.token;

public record StringToken(String value) implements Token {
    @Override
    public String toString() { return "'" + value + "'"; }
}
