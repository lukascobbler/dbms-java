package com.luka.lbdb.transactions.recoveryManagement.logRecordTypes;

import com.luka.lbdb.fileManagement.Page;
import com.luka.lbdb.logManagement.LogManager;
import com.luka.lbdb.transactions.Transaction;
import com.luka.lbdb.transactions.recoveryManagement.LogRecord;
import com.luka.lbdb.transactions.recoveryManagement.LogRecordType;

/// Helper class for representing a transaction start log record type.
///
/// Structure of the transaction start log record type
/// `<START transactionNumber>`
///
/// Example: `<START 32>`
public class StartRecord implements LogRecord {
    private final int transactionNumber;

    /// The transaction start log record type is initialized with a page
    /// of a specific structure defined in the class documentation.
    /// The constructor initializes all values that can be found in
    /// the structure at specific offsets.
    public StartRecord(Page p) {
        int transactionPosition = Integer.BYTES;
        transactionNumber = p.getInt(transactionPosition);
    }

    /// Writes out a new transaction start log type record to the log file.
    /// The structure is the same as it is defined in the class documentation.
    ///
    /// @return The log sequence number representing the newly added
    /// record to the log file.
    public static int writeToLog(LogManager logManager, int transactionNumber) {
        int transactionPosition = Integer.BYTES;

        int recordLength = transactionPosition + Integer.BYTES;
        byte[] record = new byte[recordLength];

        // page used for convenience of writing to a byte array
        Page p = new Page(record);
        p.setInt(0, LogRecordType.START.value);
        p.setInt(transactionPosition, transactionNumber);

        return logManager.append(record);
    }

    /// @return The `START` log record type.
    @Override
    public LogRecordType op() {
        return LogRecordType.START;
    }

    @Override
    public int transactionNumber() {
        return transactionNumber;
    }

    /// Does nothing as this is not an update record type.
    @Override
    public void undo(Transaction transaction) { }

    /// Does nothing as this is not an update record type.
    @Override
    public void redo(Transaction transaction) { }

    @Override
    public String toString() {
        return "<" + LogRecordType.START + " " + transactionNumber + ">";
    }
}
