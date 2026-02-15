package com.luka.simpledb.recordManagement;

/// A `RecordId` represents a record within a block.
public record RecordId(int blockNum, int slot) {
    @Override
    public String toString() {
        return "[block " + blockNum + ", slot " + slot + "]";
    }
}
