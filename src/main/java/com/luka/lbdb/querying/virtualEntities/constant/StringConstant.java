package com.luka.lbdb.querying.virtualEntities.constant;

import org.jetbrains.annotations.NotNull;

/// A string constant value implementation.
public record StringConstant(String value) implements Constant {
    @Override public @NotNull String toString() { return "'" + value + "'"; }
}