package com.luka.lbdb.parsing.statement.insert;

import com.luka.lbdb.querying.virtualEntities.constant.Constant;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/// Represents data for multiple tuple inserts.
public record AllTuplesValueInfo(List<String> fieldNames, List<List<Constant>> newTuples, boolean implicitFieldNames) {
    @Override
    public @NotNull String toString() {
        StringBuilder result = new StringBuilder();

        if (!implicitFieldNames) {
            result.append('(');
            for (String fieldName : fieldNames) result.append(fieldName).append(", ");
            result = new StringBuilder(result.substring(0, result.length() - 2));
            result.append(") ");
        }

        result.append("VALUES ");

        for (List<Constant> tuple : newTuples) {
            result.append("(");
            for (Constant value : tuple) {
                result.append(value.toString()).append(", ");
            }
            result = new StringBuilder(result.substring(0, result.length() - 2));
            result.append("), ");
        }

        result = new StringBuilder(result.substring(0, result.length() - 2));

        return result.toString();
    }
}
