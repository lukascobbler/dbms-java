package com.luka.simpledb.transactionManagement;

import com.luka.simpledb.bufferManagement.Buffer;
import com.luka.simpledb.bufferManagement.BufferManager;
import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.fileManagement.FileManager;
import com.luka.simpledb.logManagement.LogManager;
import com.luka.simpledb.transactionManagement.concurrencyManagement.ConcurrencyManager;
import com.luka.simpledb.transactionManagement.recoveryManagement.RecoveryManager;

/// A transaction represents one unit of work in the database.
/// All operations must be performed transactionally, even if they
/// consist of one database query or update. A `Transaction` object
/// manages its own buffers and locks.
public class Transaction {
    private static int nextTransactionNum = 0;

    private static final int END_OF_FILE = -1;

    private final RecoveryManager recoveryManager;
    private final ConcurrencyManager concurrencyManager;
    private final BufferManager bufferManager;
    private final FileManager fileManager;

    private final int transactionNumber;
    private final BufferList myBuffers;

    /// Initializes a transaction with a transaction id that is pooled
    /// from a global number that all transactions share. Creates a
    /// concurrency manager and a recovery manager tied to this transaction.
    public Transaction(FileManager fileManager, LogManager logManager,
                       BufferManager bufferManager) {
        this.bufferManager = bufferManager;
        this.fileManager = fileManager;
        this.concurrencyManager = new ConcurrencyManager();

        transactionNumber = nextTransactionNumber();
        myBuffers = new BufferList(bufferManager);

        this.recoveryManager = new RecoveryManager(
                this, transactionNumber, logManager, bufferManager);
    }

    /// Commits the transaction by commiting to the recovery file,
    /// releasing all held locks on all block ids and unpinning all
    /// buffers held by the transaction.
    public void commit() {
        recoveryManager.commit();
        concurrencyManager.release();
        myBuffers.unpinAll();
    }

    /// Rolls back the transaction by rolling back from the recovery file,
    /// releasing all held locks on all block ids and unpinning all
    /// buffers held by the transaction.
    public void rollback() {
        recoveryManager.rollback();
        concurrencyManager.release();
        myBuffers.unpinAll();
    }

    /// Recovers the whole system including this transaction.
    public void recover() {
        bufferManager.flushAll(transactionNumber);
        recoveryManager.recover();
    }

    /// Pin a block id from this transaction.
    public void pin(BlockId blockId) {
        myBuffers.pin(blockId);
    }

    /// Unpin a block id from this transaction.
    public void unpin(BlockId blockId) {
        myBuffers.unpin(blockId);
    }

    /// Gets an integer from a block id managed by the transaction.
    /// Firstly, shared locking of the block id is performed, then the
    /// integer is returned.
    ///
    /// @return An integer from a given block id at a given offset from a buffer
    /// managed by this transaction.
    public int getInt(BlockId blockId, int offset) {
        concurrencyManager.lockShared(blockId);
        Buffer buffer = myBuffers.getBuffer(blockId);
        return buffer.getContents().getInt(offset);
    }

    /// Gets a string from a block id managed by the transaction.
    /// Firstly, shared locking of the block id is performed, then the
    /// string is returned.
    ///
    /// @return A string from a given block id at a given offset from a buffer
    /// managed by this transaction.
    public String getString(BlockId blockId, int offset) {
        concurrencyManager.lockShared(blockId);
        Buffer buffer = myBuffers.getBuffer(blockId);
        return buffer.getContents().getString(offset);
    }

    /// Sets an integer value to a block id on an offset.
    /// Firstly, exclusive locking of the block id is performed, then the
    /// value is set. The parameter `okToLog` indicates if the value will be
    /// written out to the recovery log (false when undoing the transaction).
    /// Finally, the buffer is set to be modified with this transaction's id
    /// and the LSN returned by the recovery manager if the set was logged.
    public void setInt(BlockId blockId, int offset, int value, boolean okToLog) {
        concurrencyManager.lockExclusive(blockId);
        Buffer buffer = myBuffers.getBuffer(blockId);
        int lsn = -1;
        if (okToLog) {
            lsn = recoveryManager.setInt(buffer, offset, value);
        }
        buffer.getContents().setInt(offset, value);
        buffer.setModified(transactionNumber, lsn);
    }

    /// Sets a string value to a block id on an offset.
    /// Firstly, exclusive locking of the block id is performed, then the
    /// value is set. The parameter `okToLog` indicates if the value will be
    /// written out to the recovery log (false when undoing the transaction).
    /// Finally, the buffer is set to be modified with this transaction's id
    /// and the LSN returned by the recovery manager if the set was logged.
    public void setString(BlockId blockId, int offset, String value, boolean okToLog) {
        concurrencyManager.lockExclusive(blockId);
        Buffer buffer = myBuffers.getBuffer(blockId);
        int lsn = -1;
        if (okToLog) {
            lsn = recoveryManager.setString(buffer, offset, value);
        }
        buffer.getContents().setString(offset, value);
        buffer.setModified(transactionNumber, lsn);
    }

    /// Firstly, shared locking of the whole file is performed, then the
    /// size is returned.
    ///
    /// @return The size of the file.
    public int size(String filename) {
        BlockId dummyBlock = new BlockId(filename, END_OF_FILE);
        concurrencyManager.lockShared(dummyBlock);
        return fileManager.lengthInBlocks(filename);
    }

    /// Firstly, exclusive locking of the whole file is performed, then a
    /// new block is appended.
    public BlockId append(String filename) {
        BlockId dummyBlock = new BlockId(filename, END_OF_FILE);
        concurrencyManager.lockExclusive(dummyBlock);
        return fileManager.append(filename);
    }

    /// @return The block size of the system from the file manager.
    public int blockSize() {
        return fileManager.getBlockSize();
    }

    /// @return The number of available buffers from the buffer
    /// manager.
    public int availableBuffers() {
        return bufferManager.available();
    }

    /// @return The transaction number representing the next
    /// transaction in the system. Method is `synchronized`
    /// to ensure that two transactions don't get the
    /// same transaction number.
    private static synchronized int nextTransactionNumber() {
        nextTransactionNum++;
        return nextTransactionNum;
    }
}
