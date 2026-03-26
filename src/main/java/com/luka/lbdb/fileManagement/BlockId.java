package com.luka.lbdb.fileManagement;

import org.jetbrains.annotations.NotNull;

/// A `BlockId` object is the name to a block of a file.
/// Does not care about system's block size.
public record BlockId(String filename, int blockNum) {
    @Override
    public @NotNull String toString() {
        return "[file " + filename + ", block " + blockNum + "]";
    }
}
