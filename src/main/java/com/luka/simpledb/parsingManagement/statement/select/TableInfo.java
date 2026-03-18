package com.luka.simpledb.parsingManagement.statement.select;

import java.util.Optional;

/// Represents the table name + its renaming, if it exists.
public record TableInfo(String tableName, Optional<String> rangeVariableName) {
    public TableInfo(String tableName) {
        this(tableName, Optional.empty());
    }

    public TableInfo(String tableName, String rangeVariableName) {
        this(tableName, Optional.of(rangeVariableName));
    }

    @Override
    public String toString() {
        return tableName + (rangeVariableName.map(s -> " " + s).orElse(""));
    }
}
