package com.luka.simpledb.recordManagement;

public record RecordId(int blockNum, int slot) {
    @Override
    public String toString() {
        return "[block " + blockNum + ", slot " + slot + "]";
    }
}
