package com.luka.lbdb.records;

import org.jetbrains.annotations.NotNull;

/// A `RecordId` represents a record within a block.
public record RecordId(int blockNum, int record) {
    @Override
    public @NotNull String toString() {
        return "[block " + blockNum + ", record " + record + "]";
    }
}
