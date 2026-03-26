package com.luka.lbdb.querying.virtualEntities.constant;

import org.jetbrains.annotations.NotNull;

/// A boolean constant value implementation.
public record BooleanConstant(Boolean value) implements Constant {
    @Override public @NotNull String toString() { return value.toString().toUpperCase(); }
}
