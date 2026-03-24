package com.luka.simpledb.parsingManagement.statement;

import org.jetbrains.annotations.NotNull;

/// Represents the parsed data of `CREATE VIEW` queries.
/// `CREATE VIEW` queries need the view name, and
/// a select query that the view should be defined as.
public record CreateViewStatement(String viewName, SelectStatement selectStatement) implements Statement {
    @Override
    public @NotNull String toString() {
        return "CREATE VIEW " +
                viewName +
                " AS " +
                selectStatement.toString();
    }
}
