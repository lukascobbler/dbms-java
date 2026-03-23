package com.luka.simpledb.parsingManagement.statement;

import com.luka.simpledb.recordManagement.DatabaseType;
import com.luka.simpledb.recordManagement.schema.Schema;
import org.jetbrains.annotations.NotNull;

/// Represents the parsed data of `CREATE TABLE` queries.
/// `CREATE TABLE` queries need the new table name, and
/// a schema of that new table.
public record CreateTableStatement(String tableName, Schema schema) implements Statement {
    @Override
    public @NotNull String toString() {
        StringBuilder result = new StringBuilder("CREATE TABLE ");
        result.append(tableName).append(" (");

        for (String fieldName : schema.getFields()) {
            switch (schema.type(fieldName)) {
                case DatabaseType.INT -> result.append(fieldName).append(" INT");
                case DatabaseType.VARCHAR -> result
                        .append(fieldName)
                        .append(" VARCHAR(")
                        .append(schema.runtimeLength(fieldName))
                        .append(")");
                case DatabaseType.BOOLEAN -> result.append(fieldName).append(" BOOLEAN");
            }

            if (!schema.isNullable(fieldName)) {
                result.append(" NOT NULL");
            }

            result.append(", ");
        }

        result = new StringBuilder(result.substring(0, result.length() - 2));

        result.append(");");

        return result.toString();
    }
}
