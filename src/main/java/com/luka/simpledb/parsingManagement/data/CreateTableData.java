package com.luka.simpledb.parsingManagement.data;

import com.luka.simpledb.recordManagement.Schema;

import static java.sql.Types.*;

public record CreateTableData(String tableName, Schema schema) {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("CREATE TABLE ");
        result.append(tableName).append(" (");

        for (String fieldName : schema.getFields()) {
            switch (schema.type(fieldName)) {
                case INTEGER -> result.append(fieldName).append(" INT");
                case VARCHAR -> result
                        .append(fieldName)
                        .append(" VARCHAR(")
                        .append(schema.length(fieldName))
                        .append(")");
                case BOOLEAN -> result.append(fieldName).append(" BOOLEAN");
            }

            if (schema.isNullable(fieldName)) {
                result.append(" NOT NULL");
            }

            result.append(", ");
        }

        result = new StringBuilder(result.substring(0, result.length() - 2));

        result.append(tableName).append(");");

        return result.toString();
    }
}
