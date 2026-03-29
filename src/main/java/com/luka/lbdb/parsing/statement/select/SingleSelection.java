package com.luka.lbdb.parsing.statement.select;

import com.luka.lbdb.querying.virtualEntities.Predicate;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/// A record that represents non-unionized `SELECT` queries, meaning
/// only a single `SELECT` query. A single `SELECT` query needs a list
/// of projection fields, a list of tables and a predicate to know which
/// data to match.
public record SingleSelection(List<ProjectionFieldInfo> projectionFields, List<TableInfo> tables, Predicate predicate) {
    @Override
    public @NotNull String toString() {
        StringBuilder result = new StringBuilder("SELECT ");
        for (var projectionField : projectionFields) {
            result.append(projectionField.toString());

            result.append(", ");
        }

        result = new StringBuilder(result.substring(0, result.length() - 2));

        if (!tables.isEmpty()) {
            result.append(" FROM ");
            for (TableInfo tableInfo : tables)
                result.append(tableInfo.toString()).append(", ");

            result = new StringBuilder(result.substring(0, result.length() - 2));
        }

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

        for (TableInfo tableInfo : tables) {
            result += (tableInfo != null ? tableInfo.hashCode() : 0);
        }

        for (ProjectionFieldInfo field : projectionFields) {
            result += (field != null ? field.hashCode() : 0);
        }

        return result;
    }
}
