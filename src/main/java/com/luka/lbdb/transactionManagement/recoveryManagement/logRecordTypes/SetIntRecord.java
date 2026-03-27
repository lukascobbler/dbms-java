package com.luka.lbdb.transactionManagement.recoveryManagement.logRecordTypes;

import com.luka.lbdb.fileManagement.BlockId;
import com.luka.lbdb.fileManagement.Page;
import com.luka.lbdb.logManagement.LogManager;
import com.luka.lbdb.transactionManagement.Transaction;
import com.luka.lbdb.transactionManagement.recoveryManagement.LogRecord;
import com.luka.lbdb.transactionManagement.recoveryManagement.LogRecordType;

/// Helper class for representing a string update log record type.
///
/// Structure of the string update log record type:
/// `<SETINT transactionNumber filename blockId offset oldValue newValue>`
///
/// Example: `<SETINT 1 file1 50 100 10 100>`
public class SetIntRecord implements LogRecord {
    private final int transactionNumber;
    private final int offset;
    private final int oldValue;
    private final int newValue;
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

        int oldValuePosition = offsetPosition + Integer.BYTES;
        oldValue = p.getInt(oldValuePosition);

        int newValuePosition = oldValuePosition + Integer.BYTES;
        newValue = p.getInt(newValuePosition);
    }

    /// Writes out a new update integer log type record to the log file.
    /// The structure is the same as it is defined in the class documentation.
    ///
    /// @return The log sequence number representing the newly added
    /// record to the log file.
    public static int writeToLog(LogManager logManager, int transactionNumber,
                                 BlockId blockId, int offset, int oldValue, int newValue) {
        int logRecordTypePosition = 0;
        int transactionPosition = logRecordTypePosition + Integer.BYTES;
        int filenamePosition = transactionPosition + Integer.BYTES;
        int blockPosition = filenamePosition + Page.maxLength(blockId.filename().length());
        int offsetPosition = blockPosition + Integer.BYTES;
        int oldValuePosition = offsetPosition + Integer.BYTES;
        int newValuePosition = oldValuePosition + Integer.BYTES;

        int recordLength = newValuePosition + Integer.BYTES;
        byte[] record = new byte[recordLength];

        // page used for convenience of writing to a byte array
        Page p = new Page(record);
        p.setInt(0, LogRecordType.SETINT.value);
        p.setInt(transactionPosition, transactionNumber);
        p.setString(filenamePosition, blockId.filename());
        p.setInt(blockPosition, blockId.blockNum());
        p.setInt(offsetPosition, offset);
        p.setInt(oldValuePosition, oldValue);
        p.setInt(newValuePosition, newValue);

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
    /// logs. Only does the undo operation if the value written in the block
    /// does not match the old value.
    @Override
    public void undo(Transaction transaction) {
        transaction.pin(blockId);
        if (transaction.getInt(blockId, offset) != oldValue) {
            transaction.setInt(blockId, offset, oldValue, false);
        }
        transaction.unpin(blockId);
    }

    /// Redoes the integer update for a given transaction. Does not log the
    /// undo operations for the transaction, as that would create redundant
    /// logs. Only does the redo operation if the value written in the block
    /// does not match the new value.
    @Override
    public void redo(Transaction transaction) {
        transaction.pin(blockId);
        if (transaction.getInt(blockId, offset) != newValue) {
            transaction.setInt(blockId, offset, newValue, false);
        }
        transaction.unpin(blockId);
    }

    @Override
    public String toString() {
        return "<" + LogRecordType.SETINT + " " + transactionNumber + " " + blockId.filename()
                + " " + blockId.blockNum() + " " + offset + " " + oldValue + " " + newValue + ">";
    }
}
