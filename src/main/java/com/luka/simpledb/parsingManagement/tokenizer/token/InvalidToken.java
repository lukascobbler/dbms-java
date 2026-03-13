package com.luka.simpledb.parsingManagement.tokenizer.token;

/// A token type that represents the invalid input. Helps
/// the user to understand what is wrong in the query.
public record InvalidToken(String text) implements Token {
    @Override
    public String toString() { return text; }
}