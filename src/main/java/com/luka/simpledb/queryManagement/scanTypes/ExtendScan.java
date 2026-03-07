package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanDefinitions.UnaryScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;
import com.luka.simpledb.queryManagement.virtualEntities.expression.PartialEvaluator;

public class ExtendScan extends UnaryScan {
    private final Expression expression;
    private final String newFieldName;

    public ExtendScan(Scan childScan, Expression expression, String newFieldName) {
        super(childScan);
        this.expression = PartialEvaluator.evaluate(expression);
        this.newFieldName = newFieldName;
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldName.equals(newFieldName) || super.hasField(fieldName);
    }

    @Override
    protected int internalGetInt(String fieldName) {
        if (fieldName.equals(newFieldName)) {
            return expression.evaluate(childScan).asInt();
        }

        return super.internalGetInt(fieldName);
    }

    @Override
    protected String internalGetString(String fieldName) {
        if (fieldName.equals(newFieldName)) {
            return expression.evaluate(childScan).asString();
        }

        return super.internalGetString(fieldName);
    }

    @Override
    protected boolean internalGetBoolean(String fieldName) {
        if (fieldName.equals(newFieldName)) {
            return expression.evaluate(childScan).asBoolean();
        }

        return super.internalGetBoolean(fieldName);
    }

    @Override
    protected Constant internalGetValue(String fieldName) {
        if (fieldName.equals(newFieldName)) {
            return expression.evaluate(childScan);
        }

        return super.internalGetValue(fieldName);
    }
}
