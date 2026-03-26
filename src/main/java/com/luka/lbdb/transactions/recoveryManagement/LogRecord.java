package com.luka.lbdb.transactions.recoveryManagement;

import com.luka.lbdb.fileManagement.Page;
import com.luka.lbdb.transactions.Transaction;
import com.luka.lbdb.transactions.recoveryManagement.exceptions.LogRecordParseException;
import com.luka.lbdb.transactions.recoveryManagement.logRecordTypes.*;

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

    /// Redoes any update operation. Makes sense only for update logs.
    /// Does nothing for non-update logs.
    void redo(Transaction transaction);

    /// Encapsulates the logic of initializing a typed log record
    /// from a list of bytes. Note that potentially not every log record
    /// will need the list of bytes.
    ///
    /// @return An initialized typed log record.
    /// @throws LogRecordParseException if the found log type doesn't match any known log type.
    static LogRecord createLogRecord(byte[] bytes) {
        Page p = new Page(bytes);
        switch (LogRecordType.valueOf(p.getInt(0))) {
            case QUIESCENT_CHECKPOINT -> { return new QuiescentCheckpointRecord(p); }
            case START -> { return new StartRecord(p); }
            case COMMIT -> { return new CommitRecord(p); }
            case ROLLBACK -> { return new RollbackRecord(p); }
            case SETINT -> { return new SetIntRecord(p); }
            case SETSTRING -> { return new SetStringRecord(p); }
            case SETBOOLEAN -> { return new SetBooleanRecord(p); }
            case APPEND -> { return new AppendBlockRecord(p); }
            case null, default -> throw new LogRecordParseException();
        }
    }
}