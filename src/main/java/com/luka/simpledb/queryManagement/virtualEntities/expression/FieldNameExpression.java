package com.luka.simpledb.queryManagement.virtualEntities.expression;

import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.recordManagement.Schema;

public record FieldNameExpression(String fieldName) implements Expression {
    @Override
    public Constant evaluate(Scan scan) {
        return scan.getValue(fieldName);
    }

    @Override
    public boolean appliesTo(Schema schema) {
        return schema.hasField(fieldName);
    }

    @Override
    public String toString() {
        return fieldName;
    }
}
