package com.luka.lbdb.querying.virtualEntities.expression;

import com.luka.lbdb.querying.scanDefinitions.Scan;
import com.luka.lbdb.querying.virtualEntities.constant.Constant;
import org.jetbrains.annotations.NotNull;

/// A constant expression represents an expression wrapper over a constant.
public record ConstantExpression(Constant constant) implements Expression {
    /// A constant expression always evaluates to itself, or to be
    /// more precise, to the internal constant value.
    ///
    /// @return A fixed constant that the expression is wrapped around.
    @Override
    public Constant evaluate(Scan scan) {
        return constant;
    }

    @Override
    public @NotNull String toString() {
        return constant.toString();
    }
}
