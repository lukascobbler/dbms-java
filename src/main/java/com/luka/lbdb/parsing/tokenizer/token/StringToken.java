package com.luka.lbdb.parsing.tokenizer.token;

import org.jetbrains.annotations.NotNull;

/// A string token captures the tokenized string literals, but
/// not identifiers. String literals always have " or ' symbols
/// next to them.
public record StringToken(String value) implements Token {
    @Override
    public @NotNull String toString() { return "'" + value + "'"; }
}
