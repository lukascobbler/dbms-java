package com.luka.simpledb.fileManagement;

import java.util.Objects;

public class BlockId {
    private final String filename;
    private final int blockNum;

    public BlockId(String filename, int blockNum) {
        this.blockNum = blockNum;
        this.filename = filename;
    }

    public String getFilename() {
        return filename;
    }

    public int getBlockNum() {
        return blockNum;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        BlockId blockId1 = (BlockId) o;
        return blockNum == blockId1.blockNum && Objects.equals(filename, blockId1.filename);
    }

    @Override
    public String toString() {
        return "[file " + filename + ", block " + blockNum + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
