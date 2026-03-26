package com.luka.lbdb.parsing.statement;

import org.jetbrains.annotations.NotNull;

public record ExplainStatement(Statement explainingStatement) implements Statement {
    @Override public @NotNull String toString() {
        return "EXPLAIN " + explainingStatement.toString();
    }
}
