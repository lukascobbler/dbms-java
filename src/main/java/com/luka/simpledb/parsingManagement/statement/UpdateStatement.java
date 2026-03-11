package com.luka.simpledb.parsingManagement.statement;

import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

public record UpdateStatement(String tableName, String fieldName, Expression newValue, Predicate predicate)
        implements Statement {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("UPDATE ");
        result.append(tableName);
        result.append(" SET ");

        result.append(fieldName).append(" = ");
        result.append(newValue.toString());

        String predicateString = predicate.toString();
        if (!predicateString.isEmpty())
            result.append(" WHERE ").append(predicateString);

        result.append(';');

        return result.toString();
    }
}
