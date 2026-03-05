package com.luka.simpledb.queryManagement.virtualEntities.expression;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.recordManagement.Schema;

public sealed interface Expression permits ConstantExpression, FieldNameExpression, ArithmeticExpression {
    Constant evaluate(Scan scan);
    boolean appliesTo(Schema schema);
}
