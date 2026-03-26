package com.luka.lbdb.querying.virtualEntities.constant;

import org.jetbrains.annotations.NotNull;

/// A NULL constant value implementation.
public record NullConstant() implements Constant {
    @Override public @NotNull String toString() { return "NULL"; }

    /// Global null constant instance to prevent unnecessary allocations.
    public static final NullConstant INSTANCE = new NullConstant();
}