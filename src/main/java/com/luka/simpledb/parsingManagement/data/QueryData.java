package com.luka.simpledb.parsingManagement.data;

import com.luka.simpledb.queryManagement.virtualEntities.Predicate;

import java.util.Collection;
import java.util.List;

public record QueryData(List<String> fields, Collection<String> tables, Predicate predicate) {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("SELECT ");
        for (String fieldName : fields)
            result.append(fieldName).append(", ");

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
