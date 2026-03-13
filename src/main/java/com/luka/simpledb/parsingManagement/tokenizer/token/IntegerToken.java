package com.luka.simpledb.parsingManagement.tokenizer.token;

/// An integer token captures integer numbers.
public record IntegerToken(int value) implements Token {
    @Override
    public String toString() { return "integer(" + value + ")"; }
}