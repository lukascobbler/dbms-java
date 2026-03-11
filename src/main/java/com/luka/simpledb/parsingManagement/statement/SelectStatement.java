package com.luka.simpledb.parsingManagement.statement;

import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

import java.util.Collection;
import java.util.List;

public record SelectStatement(List<Expression> selectionExpressions,
                              Collection<String> tables, Predicate predicate) implements Statement {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("SELECT ");
        for (Expression selectionExpression : selectionExpressions)
            result.append(selectionExpression).append(", ");

        result = new StringBuilder(result.substring(0, result.length() - 2));

        result.append(" FROM ");
        for (String tableName : tables)
            result.append(tableName).append(", ");

        result = new StringBuilder(result.substring(0, result.length() - 2));

        String predicateString = predicate.toString();
        if (!predicateString.isEmpty())
            result.append(" WHERE ").append(predicateString);

        result.append(';');

        return result.toString();
    }
}
