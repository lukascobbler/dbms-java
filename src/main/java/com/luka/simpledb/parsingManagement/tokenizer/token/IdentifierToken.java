package com.luka.simpledb.parsingManagement.tokenizer.token;

public record IdentifierToken(String name) implements Token {
    @Override
    public String toString() { return "identifier(" + name + ")"; }
}
