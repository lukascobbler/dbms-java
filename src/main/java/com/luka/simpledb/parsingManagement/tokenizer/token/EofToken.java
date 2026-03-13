package com.luka.simpledb.parsingManagement.tokenizer.token;

/// Represents the end of the tokenization input. Helps
/// the user to understand what is wrong in the query.
public record EofToken() implements Token {
    @Override
    public String toString() { return "EOF"; }
}
