package com.luka.simpledb.queryManagement.virtualEntities.expression;

import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.recordManagement.Schema;

/// A field name expression represents an expression wrapper over some field
/// in a table.
public record FieldNameExpression(String fieldName) implements Expression {
    /// @return The constant value for the field name in the current scan position.
    @Override
    public Constant evaluate(Scan scan) {
        return scan.getValue(fieldName);
    }

    /// @return True if the schema contains the field defined in this field name
    /// expression.
    @Override
    public boolean appliesTo(Schema schema) {
        return schema.hasField(fieldName);
    }

    /// A field name expression is never constant.
    ///
    /// @return False.
    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public String toString() {
        return fieldName;
    }
}
