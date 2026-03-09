package com.luka.simpledb.queryManagement.virtualEntities.expression;

import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.recordManagement.Schema;

/// Expressions that the database virtual machine evaluates on concrete table
/// rows. This interface is an abstraction over all types of expressions that
/// the database can evaluate, and it is needed for constructing expression ASTs
/// whose exact structure isn't known at compile time.
public sealed interface Expression permits ConstantExpression, FieldNameExpression, ArithmeticExpression {
    /// @return The constant evaluation of an expression over some scan.
    Constant evaluate(Scan scan);
    /// @return Whether this expression does apply to this schema.
    boolean appliesTo(Schema schema);
}
