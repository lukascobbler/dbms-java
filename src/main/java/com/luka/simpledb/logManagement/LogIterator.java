package com.luka.simpledb.logManagement;

import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.fileManagement.FileManager;
import com.luka.simpledb.fileManagement.Page;

import java.util.Iterator;

/// Class responsible for iterating over logs in the log file.
class LogIterator implements Iterator<byte[]> {
    private final FileManager fileManager;
    private BlockId blockId;
    private final Page page;
    private int currentPos;

    /// Initializes the iterator to a starting block id and prepares
    /// the current position variable for reverse reading of records.
    public LogIterator(FileManager fileManager, BlockId blockId) {
        this.fileManager = fileManager;
        this.blockId = blockId;

        byte[] b = new byte[fileManager.getBlockSize()];
        page = new Page(b);
        moveToBlock(blockId);
    }

    @Override
    /// Iteration has more elements if either the current position is
    /// less than the system's block size (there are more log records in the current block)
    /// OR if the block id count hasn't reached the first block in the log file
    /// (there are more blocks in the log file).
    public boolean hasNext() {
        return currentPos < fileManager.getBlockSize() || blockId.blockNum() > 0;
    }

    @Override
    /// Returns the next log record in the sequence. The sequence is read
    /// from right to left in block terms, but from left to right inside
    /// a block (because log records are written in reverse).
    public byte[] next() {
        // if the current position reached the system's block size
        // that means we need to go to the previous block to continue
        // reading from right to left
        if (currentPos == fileManager.getBlockSize()) {
            blockId = new BlockId(blockId.filename(), blockId.blockNum() - 1);
            moveToBlock(blockId); 
        }

        // read the bytes from the current position and
        // update the current position to point to the next
        // log record (left to right reading)
        byte[] record = page.getBytes(currentPos);
        currentPos += Integer.BYTES + record.length;
        return record;
    }

    /// Reads the provided block id to the iterator's internal page,
    /// and sets the current position to be the boundary of that page.
    private void moveToBlock(BlockId blockId) {
        fileManager.read(blockId, page);
        currentPos = page.getInt(0);
    }
}
