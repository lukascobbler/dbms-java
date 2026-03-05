package com.luka.simpledb.queryManagement.virtualEntities.expressions;

import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.virtualEntities.constants.Constant;
import com.luka.simpledb.recordManagement.Schema;

public record ConstantExpression(Constant constant) implements Expression {
    @Override
    public Constant evaluate(Scan scan) {
        return constant;
    }

    @Override
    public boolean appliesTo(Schema schema) {
        return true;
    }

    @Override
    public String toString() {
        return constant.toString();
    }
}
