package com.luka.simpledb.parsingManagement.tokenizer.token;

/// All strings that aren't surrounded by " or ' are identifier
/// tokens, used for table names, field names , ...
public record IdentifierToken(String name) implements Token {
    @Override
    public String toString() { return "identifier(" + name + ")"; }
}
