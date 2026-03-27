package com.luka.lbdb.parsing.statement;

import com.luka.lbdb.parsing.statement.transaction.TransactionAction;
import org.jetbrains.annotations.NotNull;

/// Represents the parsed action on a transaction.
public record TransactionStatement(TransactionAction action) implements Statement {
    @Override
    public @NotNull String toString() {
        return action.toString();
    }
}
