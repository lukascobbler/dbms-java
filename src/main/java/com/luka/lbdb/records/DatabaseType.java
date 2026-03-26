package com.luka.lbdb.records;

import com.luka.lbdb.records.exceptions.DatabaseTypeNotImplementedException;

import java.sql.Types;

/// Different types supported by the database system.
public enum DatabaseType {
    INT(Types.INTEGER, 4),
    BOOLEAN(Types.BOOLEAN, 1),
    VARCHAR(Types.VARCHAR, -1),
    NULL(Types.NULL, 0);

    public final int sqlType;
    public final int length;
    DatabaseType(int sqlType, int length) { this.sqlType = sqlType; this.length = length; }
    @Override public String toString() {
        return this.name() + "(" + length + ")";
    }

    public static DatabaseType get(int sqlType) {
        return switch (sqlType) {
            case Types.INTEGER -> DatabaseType.INT;
            case Types.VARCHAR -> DatabaseType.VARCHAR;
            case Types.BOOLEAN -> DatabaseType.BOOLEAN;
            case Types.NULL -> DatabaseType.NULL;
            default -> throw new DatabaseTypeNotImplementedException();
        };
    }
}
