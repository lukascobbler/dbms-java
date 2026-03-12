package com.luka.simpledb.parsingManagement.statement;

import com.luka.simpledb.parsingManagement.statement.update.NewFieldExpressionAssignment;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;

import java.util.List;

public record UpdateStatement(String tableName, List<NewFieldExpressionAssignment> newValues, Predicate predicate)
        implements Statement {
    @Override
    public String toString() {
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
