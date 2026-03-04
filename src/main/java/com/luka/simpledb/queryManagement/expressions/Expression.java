package com.luka.simpledb.queryManagement.expressions;

import com.luka.simpledb.queryManagement.scanTypes.Scan;
import com.luka.simpledb.recordManagement.Schema;

public class Expression {
    private Constant value = null;
    private String fieldName = null;

    public Expression(Constant value) {
        this.value = value;
    }

    public Expression(String fieldName) {
        this.fieldName = fieldName;
    }

    public boolean isFieldName() {
        return fieldName != null;
    }

    public Constant asConstant() {
        return value;
    }

    public String asFieldName() {
        return fieldName;
    }

    public Constant evaluate(Scan scan) {
        if (fieldName == null) {
            return value;
        }
        if (value == null) {
            return scan.getValue(fieldName);
        }

        return new NullConstant();
    }

    public boolean appliesTo(Schema schema) {
        if (fieldName == null) {
            return true;
        }
        if (value == null) {
            return schema.hasField(fieldName);
        }

        return false;
    }

    public String toString() {
        if (fieldName == null) {
            return value.asString();
        }
        if (value == null) {
            return fieldName;
        }

        return "";
    }
}
