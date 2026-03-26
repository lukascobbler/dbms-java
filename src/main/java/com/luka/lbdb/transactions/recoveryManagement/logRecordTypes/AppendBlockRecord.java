package com.luka.lbdb.transactions.recoveryManagement.logRecordTypes;

import com.luka.lbdb.fileManagement.BlockId;
import com.luka.lbdb.fileManagement.Page;
import com.luka.lbdb.logManagement.LogManager;
import com.luka.lbdb.transactions.Transaction;
import com.luka.lbdb.transactions.recoveryManagement.LogRecord;
import com.luka.lbdb.transactions.recoveryManagement.LogRecordType;

/// Helper class for representing an append block log record type.
///
/// Structure of the append block log record type:
/// `<APPEND transactionNumber filename blockId>`
///
/// Example: `<APPEND 1 file1 47>`
public class AppendBlockRecord implements LogRecord {
    private final int transactionNumber;
    private final BlockId lastBlock;

    /// The append block log record type is initialized with a page
    /// of a specific structure defined in the class documentation.
    /// The constructor initializes all values that can be found in
    /// the structure at specific offsets.
    public AppendBlockRecord(Page p) {
        int transactionPosition = Integer.BYTES;
        transactionNumber = p.getInt(transactionPosition);

        int filenamePosition = transactionPosition + Integer.BYTES;
        String filename = p.getString(filenamePosition);

        int blockPosition = filenamePosition + Page.maxLength(filename.length());
        int blockNumber = p.getInt(blockPosition);
        lastBlock = new BlockId(filename, blockNumber);
    }

    /// Writes out a new append block log type record to the log file.
    /// The structure is the same as it is defined in the class documentation.
    ///
    /// @return The log sequence number representing the newly added
    /// record to the log file.
    public static int writeToLog(LogManager logManager, int transactionNumber, BlockId blockId) {
        int logRecordTypePosition = 0;
        int transactionPosition = logRecordTypePosition + Integer.BYTES;
        int filenamePosition = transactionPosition + Integer.BYTES;
        int blockPosition = filenamePosition + Page.maxLength(blockId.filename().length());

        int recordLength = blockPosition + Integer.BYTES;
        byte[] record = new byte[recordLength];

        // page used for convenience of writing to a byte array
        Page p = new Page(record);
        p.setInt(0, LogRecordType.APPEND.value);
        p.setInt(transactionPosition, transactionNumber);
        p.setString(filenamePosition, blockId.filename());
        p.setInt(blockPosition, blockId.blockNum());

        return logManager.append(record);
    }

    /// @return The `APPEND` log record type.
    @Override
    public LogRecordType op() {
        return LogRecordType.APPEND;
    }

    @Override
    public int transactionNumber() {
        return transactionNumber;
    }

    /// Undoes the append block for a given transaction.
    @Override
    public void undo(Transaction transaction) {
        transaction.pin(lastBlock);
        transaction.undoAppendBlock(lastBlock);
        transaction.unpin(lastBlock);
    }

    /// Redoing of append block operations is not done because
    /// if a transaction completed, the correct number of blocks
    /// exist in a file, since block appending is an eager operation.
    @Override
    public void redo(Transaction transaction) { }

    @Override
    public String toString() {
        return "<" + LogRecordType.APPEND + " " + transactionNumber + " " + lastBlock.filename() + " " + lastBlock.blockNum() + ">";
    }
}
