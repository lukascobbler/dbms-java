package com.luka.lbdb.parsing.statement;

import com.luka.lbdb.parsing.statement.select.SingleSelection;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/// Represents the parsed data of `SELECT` queries.
/// `SELECT` queries need consist of smaller select
/// queries that are joined by the `UNION` keyword.
public record SelectStatement(List<SingleSelection> unionizedSelections) implements Statement {
    @Override
    public @NotNull String toString() {
        StringBuilder result = new StringBuilder();

        for (SingleSelection singleSelection : unionizedSelections) {
            result.append(singleSelection.toString()).append(" UNION ALL ");
        }

        result = new StringBuilder(result.substring(0, result.length() - 11));
        result.append(';');

        return result.toString();
    }
}
