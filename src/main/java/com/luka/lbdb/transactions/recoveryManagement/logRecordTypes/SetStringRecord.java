package com.luka.lbdb.transactions.recoveryManagement.logRecordTypes;

import com.luka.lbdb.fileManagement.BlockId;
import com.luka.lbdb.fileManagement.Page;
import com.luka.lbdb.logManagement.LogManager;
import com.luka.lbdb.transactions.Transaction;
import com.luka.lbdb.transactions.recoveryManagement.LogRecord;
import com.luka.lbdb.transactions.recoveryManagement.LogRecordType;

/// Helper class for representing a string update log record type.
///
/// Structure of the string update log record type (when the system is configured
/// to use the 'Undo-Redo' recovery algorithm):
/// `<SETSTRING transactionNumber filename blockId offset oldValue newValue>`
///
/// Example: `<SETSTRING 1 file1 50 100 no yes>`
public class SetStringRecord implements LogRecord {
    private final int transactionNumber;
    private final int offset;
    private final String oldValue;
    private final String newValue;
    private final BlockId blockId;

    /// The string update log record type is initialized with a page
    /// of a specific structure defined in the class documentation.
    /// The constructor initializes all values that can be found in
    /// the structure at specific offsets.
    public SetStringRecord(Page p) {
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
        oldValue = p.getString(oldValuePosition);

        int newValuePosition = oldValuePosition + Integer.BYTES;
        newValue = p.getString(newValuePosition);
    }

    /// Writes out a new update string log type record to the log file.
    /// The structure is the same as it is defined in the class documentation.
    ///
    /// @return The log sequence number representing the newly added
    /// record to the log file.
    public static int writeToLog(LogManager logManager, int transactionNumber,
                                 BlockId blockId, int offset, String oldValue, String newValue) {
        int logRecordTypePosition = 0;
        int transactionPosition = logRecordTypePosition + Integer.BYTES;
        int filenamePosition = transactionPosition + Integer.BYTES;
        int blockPosition = filenamePosition + Page.maxLength(blockId.filename().length());
        int offsetPosition = blockPosition + Integer.BYTES;
        int oldValuePosition = offsetPosition + Integer.BYTES;
        int newValuePosition = oldValuePosition + Page.maxLength(oldValue.length());

        int recordLength = newValuePosition + Page.maxLength(newValue.length()) + Integer.BYTES;
        byte[] record = new byte[recordLength];

        // page used for convenience of writing to a byte array
        Page p = new Page(record);
        p.setInt(0, LogRecordType.SETSTRING.value);
        p.setInt(transactionPosition, transactionNumber);
        p.setString(filenamePosition, blockId.filename());
        p.setInt(blockPosition, blockId.blockNum());
        p.setInt(offsetPosition, offset);
        p.setString(oldValuePosition, oldValue);
        p.setString(newValuePosition, newValue);

        return logManager.append(record);
    }

    /// @return The `SETSTRING` log record type.
    @Override
    public LogRecordType op() {
        return LogRecordType.SETSTRING;
    }

    @Override
    public int transactionNumber() {
        return transactionNumber;
    }

    /// Undoes the string update for a given transaction. Does not log the
    /// undo operations for the transaction, as that would create redundant
    /// logs. Only does the undo operation if the value written in the block
    /// does not match the old value.
    @Override
    public void undo(Transaction transaction) {
        transaction.pin(blockId);
        if (!transaction.getString(blockId, offset).equals(oldValue)) {
            transaction.setString(blockId, offset, oldValue, false);
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
        if (!transaction.getString(blockId, offset).equals(newValue)) {
            transaction.setString(blockId, offset, newValue, false);
        }
        transaction.unpin(blockId);
    }

    @Override
    public String toString() {
        return "<" + LogRecordType.SETSTRING + " " + transactionNumber
               + " " + blockId.filename() + " " + blockId.blockNum() + " "
                + offset + " " + oldValue +  newValue + ">";
    }
}
