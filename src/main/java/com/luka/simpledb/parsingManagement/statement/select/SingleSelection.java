package com.luka.simpledb.parsingManagement.statement.select;

import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

import java.util.Collection;
import java.util.Map;

public record SingleSelection(Map<String, Expression> selectionExpressions,
                              Collection<String> tables, Predicate predicate) {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("SELECT ");
        for (var expressionEntry : selectionExpressions.entrySet()) {
            result.append(expressionEntry.getValue().toString());

            if (!expressionEntry.getValue().toString().equals(expressionEntry.getKey())) {
                result.append(" AS ").append(expressionEntry.getKey());
            }

            result.append(", ");
        }

        result = new StringBuilder(result.substring(0, result.length() - 2));

        result.append(" FROM ");
        for (String tableName : tables)
            result.append(tableName).append(", ");

        result = new StringBuilder(result.substring(0, result.length() - 2));

        String predicateString = predicate.toString();
        if (!predicateString.isEmpty())
            result.append(" WHERE ").append(predicateString);

        return result.toString();
    }
}
