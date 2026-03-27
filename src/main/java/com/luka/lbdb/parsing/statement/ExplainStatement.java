package com.luka.lbdb.parsing.statement;

import com.luka.lbdb.records.schema.Schema;
import org.jetbrains.annotations.NotNull;

public record ExplainStatement(Statement explainingStatement) implements Statement {
    @Override public @NotNull String toString() {
        return "EXPLAIN " + explainingStatement.toString();
    }

    /// Always-same schema that encapsulates all fields the explain
    /// statement requires.
    ///
    /// @return The schema describing explain statement's fields.
    public static Schema ExplainStatementSchema() {
        Schema schema = new Schema();
        schema.addStringField("Scan", 100, false);
        schema.addStringField("Block est.", 100, false);
        schema.addStringField("Record est.", 100, false);
        schema.addStringField("Details", 100, false);

        return schema;
    }
}
