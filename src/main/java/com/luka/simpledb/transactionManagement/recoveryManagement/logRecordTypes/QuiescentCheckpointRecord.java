package com.luka.simpledb.transactionManagement.recoveryManagement.logRecordTypes;

import com.luka.simpledb.fileManagement.Page;
import com.luka.simpledb.logManagement.LogManager;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.simpledb.transactionManagement.recoveryManagement.LogRecord;
import com.luka.simpledb.transactionManagement.recoveryManagement.LogRecordType;

/// Helper class for representing a quiescent checkpoint log record type.
/// Has a default constructor since it doesn't have any internal state.
///
/// Structure of the transaction start log record type
/// `<QUIESCENT_CHECKPOINT lastTransactionNumber>`
///
/// Example: `<QUIESCENT_CHECKPOINT 20>`
public class QuiescentCheckpointRecord implements LogRecord {
    private final int lastTransactionNumber;

    /// The quiescent checkpoint log record type is initialized with a page
    /// of a specific structure defined in the class documentation.
    /// The constructor initializes all values that can be found in
    /// the structure at specific offsets.
    public QuiescentCheckpointRecord(Page p) {
        int lastTransactionPosition = Integer.BYTES;
        lastTransactionNumber = p.getInt(lastTransactionPosition);
    }

    /// Writes out a new transaction start log type record to the log file.
    /// The structure is the same as it is defined in the class documentation.
    ///
    /// @return The log sequence number representing the newly added
    /// record to the log file.
    public static int writeToLog(LogManager logManager, int lastTransactionNumber) {
        int lastTransactionPosition = Integer.BYTES;

        int recordLength = lastTransactionPosition + Integer.BYTES;
        byte[] record = new byte[recordLength];

        // page used for convenience of writing to a byte array
        Page p = new Page(record);
        p.setInt(0, LogRecordType.QUIESCENT_CHECKPOINT.value);
        p.setInt(lastTransactionPosition, lastTransactionNumber);

        return logManager.append(record);
    }

    /// @return The `QUIESCENT_CHECKPOINT` log record type.
    @Override
    public LogRecordType op() {
        return LogRecordType.QUIESCENT_CHECKPOINT;
    }

    /// Quiescent checkpoints don't have any transaction in specific related to them.
    ///
    /// @return -1 as the invalid transaction number.
    @Override
    public int transactionNumber() {
        return -1;
    }

    /// Does nothing as this is not an update record type.
    @Override
    public void undo(Transaction transaction) { }

    /// Does nothing as this is not an update record type.
    @Override
    public void redo(Transaction transaction) { }

    @Override
    public String toString() {
        return "<" + LogRecordType.QUIESCENT_CHECKPOINT + " " + lastTransactionNumber + ">";
    }

    public int getLastTransactionNumber() {
        return lastTransactionNumber;
    }
}
