package com.luka.simpledb.parsingManagement.statement.select;

import com.luka.simpledb.queryManagement.virtualEntities.Predicate;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public record SingleSelection(List<ProjectionFieldInfo> projectionFields,
                              Collection<String> tables, Predicate predicate) {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("SELECT ");
        for (var projectionField : projectionFields) {
            result.append(projectionField.toString());

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SingleSelection that = (SingleSelection) o;

        if (!Objects.equals(predicate, that.predicate)) return false;

        if (tables.size() != that.tables.size()) return false;
        if (!new HashSet<>(tables).equals(new HashSet<>(that.tables))) return false;

        if (projectionFields.size() != that.projectionFields.size()) return false;
        return new HashSet<>(projectionFields).equals(new HashSet<>(that.projectionFields));
    }

    @Override
    public int hashCode() {
        int result = (predicate != null ? predicate.hashCode() : 0);

        for (String table : tables) {
            result += (table != null ? table.hashCode() : 0);
        }

        for (ProjectionFieldInfo field : projectionFields) {
            result += (field != null ? field.hashCode() : 0);
        }

        return result;
    }
}
