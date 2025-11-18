package com.luka.simpledb.transactionManagement;

import com.luka.simpledb.bufferManagement.BufferManager;
import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.fileManagement.FileManager;
import com.luka.simpledb.logManagement.LogManager;

public class Transaction {
    public Transaction(FileManager fileManager, LogManager logManager, BufferManager bufferManager) {}

    public void commit() {}
    public void rollback() {}
    public void recover() {}

    public void pin(BlockId blockId) {}
    public void unpin(BlockId blockId) {}
    public int getInt(BlockId blockId, int offset) { return 0; }
    public String getString(BlockId blockId, int offset) { return ""; }
    public void setInt(BlockId blockId, int offset, int value, boolean okToLog) {}
    public void setString(BlockId blockId, int offset, String value, boolean okToLog) {}
    public int availableBuffers() { return 0; }

    public int size(String filename) { return 0; }
    public BlockId append(String filename) { return new BlockId("", 0); }
    public int blockSize() { return 0; }
}
