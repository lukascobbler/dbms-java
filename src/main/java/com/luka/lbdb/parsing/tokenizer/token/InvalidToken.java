package com.luka.lbdb.parsing.tokenizer.token;

import org.jetbrains.annotations.NotNull;

/// A token type that represents the invalid input. Helps
/// the user to understand what is wrong in the query.
public record InvalidToken(String text) implements Token {
    @Override
    public @NotNull String toString() { return text; }
}