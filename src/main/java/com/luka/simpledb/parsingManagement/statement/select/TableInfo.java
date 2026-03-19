package com.luka.simpledb.parsingManagement.statement.select;

import java.util.Optional;

/// Represents the table name + its renaming, if it exists.
public record TableInfo(String tableName, Optional<String> rangeVariableName) {
    /// Instantiating a table info only with a table name.
    public TableInfo(String tableName) {
        this(tableName, Optional.empty());
    }

    /// Instantiating a table info with a range variable ana a table name.
    public TableInfo(String tableName, String rangeVariableName) {
        this(tableName, Optional.of(rangeVariableName));
    }

    /// @return The string that the table goes by.
    public String qualifier() {
        return rangeVariableName.orElse(tableName);
    }

    @Override
    public String toString() {
        return tableName + (rangeVariableName.map(s -> " " + s).orElse(""));
    }
}
