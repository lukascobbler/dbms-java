package com.luka.simpledb.transactionManagement.recoveryManagement;

import com.luka.simpledb.fileManagement.Page;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.simpledb.transactionManagement.recoveryManagement.exceptions.LogRecordParseException;
import com.luka.simpledb.transactionManagement.recoveryManagement.logRecordTypes.*;

/// The log file will consist of log records. Different log records
/// exist for different purposes. The `LogRecord` interface defines
/// shared and semi-shared behavior for all types of records.
public interface LogRecord {
    /// @return The operation of the log record.
    LogRecordType op();

    /// @return The transaction number associated with the log record. Returns `-1`
    /// for log record types that don't have a transaction number in them.
    int transactionNumber();

    /// Undoes any update operation. Makes sense only for update logs.
    /// Does nothing for non-update log records.
    void undo(Transaction transaction);

    /// Encapsulates the logic of initializing a typed log record
    /// from a list of bytes. Note that potentially not every log record
    /// will need the list of bytes.
    ///
    /// @return An initialized typed log record.
    static LogRecord createLogRecord(byte[] bytes) {
        Page p = new Page(bytes);
        switch (LogRecordType.valueOf(p.getInt(0))) {
            case NON_QUIESCENT_CHECKPOINT -> { return new NonQuiescentCheckpointRecord(p); }
            case QUIESCENT_CHECKPOINT -> { return new QuiescentCheckpointRecord(); }
            case START -> { return new StartRecord(p); }
            case COMMIT -> { return new CommitRecord(p); }
            case ROLLBACK -> { return new RollbackRecord(p); }
            case SETINT -> { return new SetIntRecord(p); }
            case SETSTRING -> { return new SetStringRecord(p); }
            case null, default -> throw new LogRecordParseException();
        }
    }
}