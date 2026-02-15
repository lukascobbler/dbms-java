package com.luka.simpledb.fileManagement;

/// A `BlockId` object is the name to a block of a file.
/// Does not care about system's block size.
public record BlockId(String filename, int blockNum) {
    @Override
    public String toString() {
        return "[file " + filename + ", block " + blockNum + "]";
    }
}
