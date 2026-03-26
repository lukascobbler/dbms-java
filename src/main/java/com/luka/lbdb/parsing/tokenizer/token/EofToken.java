package com.luka.lbdb.parsing.tokenizer.token;

import org.jetbrains.annotations.NotNull;

/// Represents the end of the tokenization input. Helps
/// the user to understand what is wrong in the query.
public record EofToken() implements Token {
    @Override
    public @NotNull String toString() { return "EOF"; }
}
