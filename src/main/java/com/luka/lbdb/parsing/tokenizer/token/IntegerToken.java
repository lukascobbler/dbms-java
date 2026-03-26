package com.luka.lbdb.parsing.tokenizer.token;

import org.jetbrains.annotations.NotNull;

/// An integer token captures integer numbers.
public record IntegerToken(int value) implements Token {
    @Override
    public @NotNull String toString() { return "integer(" + value + ")"; }
}