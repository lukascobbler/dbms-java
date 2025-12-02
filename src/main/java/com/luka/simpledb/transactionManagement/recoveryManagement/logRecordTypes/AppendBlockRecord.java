package com.luka.simpledb.transactionManagement.recoveryManagement.logRecordTypes;

import com.luka.simpledb.fileManagement.Page;
import com.luka.simpledb.logManagement.LogManager;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.simpledb.transactionManagement.recoveryManagement.LogRecord;
import com.luka.simpledb.transactionManagement.recoveryManagement.LogRecordType;

/// Helper class for representing an append block log record type.
///
/// Structure of the append block log record type:
/// `<APPEND transactionNumber filename>`
///
/// Example: `<APPEND 1 file1>`
public class AppendBlockRecord implements LogRecord {
    private final int transactionNumber;
    private final String filename;

    /// The append block log record type is initialized with a page
    /// of a specific structure defined in the class documentation.
    /// The constructor initializes all values that can be found in
    /// the structure at specific offsets.
    public AppendBlockRecord(Page p) {
        int transactionPosition = Integer.BYTES;
        transactionNumber = p.getInt(transactionPosition);

        int filenamePosition = transactionPosition + Integer.BYTES;
        filename = p.getString(filenamePosition);
    }

    /// Writes out a new append block log type record to the log file.
    /// The structure is the same as it is defined in the class documentation.
    ///
    /// @return The log sequence number representing the newly added
    /// record to the log file.
    public static int writeToLog(LogManager logManager, int transactionNumber, String filename) {
        int logRecordTypePosition = 0;
        int transactionPosition = logRecordTypePosition + Integer.BYTES;
        int filenamePosition = transactionPosition + Integer.BYTES;

        int recordLength = filenamePosition + Integer.BYTES;
        byte[] record = new byte[recordLength];

        // page used for convenience of writing to a byte array
        Page p = new Page(record);
        p.setInt(0, LogRecordType.APPEND.value);
        p.setInt(transactionPosition, transactionNumber);
        p.setString(filenamePosition, filename);

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
        transaction.truncate(filename);
    }

    /// Redoes the append block for a given transaction. Does not log the
    /// undo operations for the transaction, as that would create redundant
    /// logs. Only does the redo operation if the value written in the block
    /// does not match the new value.
    @Override
    public void redo(Transaction transaction) {
        transaction.append(filename, false);
    }

    @Override
    public String toString() {
        return "<" + LogRecordType.APPEND + " " + transactionNumber + " " + filename + ">";
    }
}
