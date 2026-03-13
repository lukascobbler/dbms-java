package com.luka.simpledb.parsingManagement.tokenizer.token;

public record InvalidToken(String text) implements Token {
    @Override
    public String toString() { return text; }
}