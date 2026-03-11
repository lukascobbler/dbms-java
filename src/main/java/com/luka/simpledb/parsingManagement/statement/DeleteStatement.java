package com.luka.simpledb.parsingManagement.statement;

import com.luka.simpledb.queryManagement.virtualEntities.Predicate;

public record DeleteStatement(String tableName, Predicate predicate) implements Statement {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("DELETE FROM ");
        result.append(tableName);

        String predicateString = predicate.toString();
        if (!predicateString.isEmpty())
            result.append(" WHERE ").append(predicateString);

        result.append(';');

        return result.toString();
    }
}
