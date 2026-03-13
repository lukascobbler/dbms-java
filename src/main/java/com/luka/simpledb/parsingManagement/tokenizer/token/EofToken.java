package com.luka.simpledb.parsingManagement.tokenizer.token;

public record EofToken() implements Token {
    @Override
    public String toString() { return "EOF"; }
}
