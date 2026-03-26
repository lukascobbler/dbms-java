package com.luka.lbdb.querying.virtualEntities.constant;

import org.jetbrains.annotations.NotNull;

/// An integer constant value implementation.
public record IntConstant(Integer value) implements Constant {
    @Override public @NotNull String toString() { return value.toString(); }
}
