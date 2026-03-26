package com.luka.lbdb.parsing.tokenizer.token;

import org.jetbrains.annotations.NotNull;

/// All strings that aren't surrounded by " or ' are identifier
/// tokens, used for table names, field names , ...
public record IdentifierToken(String name) implements Token {
    @Override
    public @NotNull String toString() { return "identifier(" + name + ")"; }
}
