package com.luka.lbdb.parsing.statement;

import com.luka.lbdb.parsing.statement.insert.AllTuplesValueInfo;
import org.jetbrains.annotations.NotNull;

/// Represents the parsed data of `INSERT` queries.
/// `INSERT` queries need the table they are inserting into,
/// a list of fields, and a list of constant values that these
/// fields should be set to in the new row.
public record InsertStatement(String tableName, AllTuplesValueInfo allTuplesValueInfo) implements Statement {
    @Override
    public @NotNull String toString() {
        return "INSERT INTO " + tableName + " " +
                allTuplesValueInfo.toString() +
                ';';
    }
}
