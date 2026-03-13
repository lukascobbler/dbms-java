package com.luka.simpledb.parsingManagement.tokenizer.token;

/// A string token captures the tokenized string literals, but
/// not identifiers. String literals always have " or ' symbols
/// next to them.
public record StringToken(String value) implements Token {
    @Override
    public String toString() { return "'" + value + "'"; }
}
