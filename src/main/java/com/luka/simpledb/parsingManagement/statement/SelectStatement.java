package com.luka.simpledb.parsingManagement.statement;

import com.luka.simpledb.parsingManagement.statement.select.SingleSelection;
import java.util.List;

/// Represents the parsed data of `SELECT` queries.
/// `SELECT` queries need consist of smaller select
/// queries that are joined by the `UNION` keyword.
public record SelectStatement(List<SingleSelection> unionizedSelections) implements Statement {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();

        for (SingleSelection singleSelection : unionizedSelections) {
            result.append(singleSelection.toString()).append(" UNION ");
        }

        result = new StringBuilder(result.substring(0, result.length() - 7));
        result.append(';');

        return result.toString();
    }
}
