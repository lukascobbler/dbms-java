package com.luka.lbdb.parsing.statement;

import com.luka.lbdb.parsing.statement.update.NewFieldExpressionAssignment;
import com.luka.lbdb.querying.virtualEntities.Predicate;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/// Represents the parsed data of `UPDATE` queries.
/// `UPDATE` queries need the table they are updating,
/// a list of field assignments to new values and a
/// predicate to decide which data to update.
public record UpdateStatement(String tableName, List<NewFieldExpressionAssignment> newValues, Predicate predicate)
        implements Statement {
    @Override
    public @NotNull String toString() {
        StringBuilder result = new StringBuilder("UPDATE ");
        result.append(tableName);
        result.append(" SET ");

        for (var newValue: newValues) {
            result.append(newValue.toString()).append(", ");
        }

        result = new StringBuilder(result.substring(0, result.length() - 2));

        String predicateString = predicate.toString();
        if (!predicateString.isEmpty())
            result.append(" WHERE ").append(predicateString);

        result.append(';');

        return result.toString();
    }
}
