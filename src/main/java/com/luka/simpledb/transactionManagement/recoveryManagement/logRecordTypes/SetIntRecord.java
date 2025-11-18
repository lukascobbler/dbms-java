package com.luka.simpledb.transactionManagement.recoveryManagement.logRecordTypes;

import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.fileManagement.Page;
import com.luka.simpledb.logManagement.LogManager;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.simpledb.transactionManagement.recoveryManagement.LogRecord;
import com.luka.simpledb.transactionManagement.recoveryManagement.LogRecordType;

/// Helper class for representing a string update log record type.
///
/// Structure of the string update log record type (when the system is configured
/// to use the 'Undo-Only' recovery algorithm):
/// `<SETINT transactionNumber blockId offset oldValue>`
///
/// Example: `<SETINT 1 50 100 10>`
///
/// Structure of the string update log record type (when the system is configured
/// to use the 'Undo-Redo' recovery algorithm):
/// `<SETINT transactionNumber blockId offset oldValue newValue>`
///
/// Example: `<SETINT 1 50 100 10 100>`
public class SetIntRecord implements LogRecord {
    private final int transactionNumber;
    private final int offset;
    private final int value;
    private final BlockId blockId;

    /// The integer update log record type is initialized with a page
    /// of a specific structure defined in the class documentation.
    /// The constructor initializes all values that can be found in
    /// the structure at specific offsets.
    public SetIntRecord(Page p) {
        int transactionPosition = Integer.BYTES;
        transactionNumber = p.getInt(transactionPosition);

        int filenamePosition = transactionPosition + Integer.BYTES;
        String filename = p.getString(filenamePosition);

        int blockPosition = filenamePosition + Page.maxLength(filename.length());
        int blockNumber = p.getInt(blockPosition);
        blockId = new BlockId(filename, blockNumber);

        int offsetPosition = blockPosition + Integer.BYTES;
        offset = p.getInt(offsetPosition);

        int valuePosition = offsetPosition + Integer.BYTES;
        value = p.getInt(valuePosition);
    }

    /// Writes out a new update integer log type record to the log file.
    /// The structure is the same as it is defined in the class documentation.
    ///
    /// @return The log sequence number representing the newly added
    /// record to the log file.
    public static int writeToLog(LogManager logManager, int transactionNumber,
                                 BlockId blockId, int offset, int value) {
        int transactionPosition = Integer.BYTES;
        int filenamePosition = transactionPosition + Integer.BYTES;
        int blockPosition = filenamePosition + Page.maxLength(blockId.filename().length());
        int offsetPosition = blockPosition + Integer.BYTES;
        int valuePosition = offsetPosition + Integer.BYTES;

        int recordLength = valuePosition + Integer.BYTES;
        byte[] record = new byte[recordLength];

        // page used for convenience of writing to a byte array
        Page p = new Page(record);
        p.setInt(0, LogRecordType.SETSTRING.value);
        p.setInt(transactionPosition, transactionNumber);
        p.setString(filenamePosition, blockId.filename());
        p.setInt(blockPosition, blockId.blockNum());
        p.setInt(offsetPosition, offset);
        p.setInt(valuePosition, value);

        return logManager.append(record);
    }

    /// @return The `SETINT` log record type.
    @Override
    public LogRecordType op() {
        return LogRecordType.SETINT;
    }

    @Override
    public int transactionNumber() {
        return transactionNumber;
    }

    /// Undoes the integer update for a given transaction. Does not log the
    /// undo operations for the transaction, as that would create redundant
    /// logs.
    @Override
    public void undo(Transaction transaction) {
        transaction.pin(blockId);
        transaction.setInt(blockId, offset, value, false);
        transaction.unpin(blockId);
    }

    @Override
    public String toString() {
        return "<" + LogRecordType.SETINT + " " + transactionNumber
                + " " + blockId + " " + offset + " " + value + ">";
    }
}
