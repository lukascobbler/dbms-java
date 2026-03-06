package com.luka.simpledb.recordManagement;

/// A `RecordId` represents a record within a block.
public record RecordId(int blockNum, int record) {
    @Override
    public String toString() {
        return "[block " + blockNum + ", record " + record + "]";
    }
}
