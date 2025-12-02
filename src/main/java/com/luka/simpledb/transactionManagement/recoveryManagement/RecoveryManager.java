package com.luka.simpledb.transactionManagement.recoveryManagement;

import com.luka.simpledb.bufferManagement.Buffer;
import com.luka.simpledb.bufferManagement.BufferManager;
import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.logManagement.LogManager;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.simpledb.transactionManagement.recoveryManagement.logRecordTypes.*;

import java.util.*;

/// The `RecoveryManager` object is used by the DBMS to handle:
/// 1. Constructing and writing log entries at appropriate times;
/// 2. Doing transaction rollbacks;
/// 3. Doing system recovery.
public class RecoveryManager {
    private final LogManager logManager;
    private final BufferManager bufferManager;
    private final Transaction transaction;
    private final int transactionNumber;
    private final RecoveryAlgorithm recoveryAlgorithm = new RecoveryAlgorithm(true);

    /// A recovery manager is instantiated with a given transaction, and is
    /// tied to it until the end of its lifecycle. A buffer manager is needed
    /// for flushing transactions, and a log manager is needed for writing out
    /// log entries. On 'RecoveryManager' instantiation, a start log record type
    /// is written out to the log file for the tied transaction.
    public RecoveryManager(Transaction transaction, int transactionNumber,
                           LogManager logManager, BufferManager bufferManager) {
        this.transaction = transaction;
        this.transactionNumber = transactionNumber;
        this.logManager = logManager;
        this.bufferManager = bufferManager;
        StartRecord.writeToLog(logManager, transactionNumber);
    }

    /// Commits the tied transaction.
    public void commit() {
        if (recoveryAlgorithm.undoOnly) {
            // the commit log record must be written **after** all buffers used by the transaction
            // are flushed, because that is what the 'Undo-Only' algorithm assumes (the assumption is
            // that all commited transactions will have their changes written out in user data blocks
            // meaning they won't need to be reapplied on system recovery); this has a penalty on performance
            bufferManager.flushAll(transactionNumber);
        }
        int lsn = CommitRecord.writeToLog(logManager, transactionNumber);
        logManager.flush(lsn);
    }

    /// Rolls back the tied transaction. Immediately writes out the rollback to
    /// disk.
    public void rollback() {
        transactionRollback();
        bufferManager.flushAll(transactionNumber);
        int lsn = RollbackRecord.writeToLog(logManager, transactionNumber);
        logManager.flush(lsn);
    }

    /// Recovers the database to a reasonable state. A reasonable state can
    /// be characterized as having two properties:
    /// - all uncompleted transactions should be rolled back (*undo* all of them);
    /// - all commited transactions should have their modifications written to disk (*redo* all of them).
    /// The second assumption will always be met because of the 'Undo-Only' recovery
    /// algorithm modification. When the system is finished with recovery, all undo operations
    /// performed are written to the disk and a quiescent checkpoint can be written out since
    /// the system is sure that the database is in a reasonable state.
    ///
    /// @return The transaction number latest in the log so that new transactions can start from this one + 1.
    public int recover() {
        int biggestTransactionNumber = recoveryAlgorithm.systemRecovery();
        bufferManager.flushAll(transactionNumber);
        int lsn = QuiescentCheckpointRecord.writeToLog(logManager);
        logManager.flush(lsn);

        return biggestTransactionNumber;
    }

    /// Creates an update log for updating an integer in a given buffer on a given offset.
    ///
    /// @return The log sequence number for the record in the log file corresponding to the
    /// call of this function.
    public int setInt(Buffer buffer, int offset, int newValue) {
        int oldValue = buffer.getContents().getInt(offset);
        BlockId blockId = buffer.getBlockId();
        return SetIntRecord.writeToLog(logManager, transactionNumber, blockId, offset, oldValue, newValue);
    }

    /// Creates an update log for updating a string in a given buffer on a given offset.
    ///
    /// @return The log sequence number for the record in the log file corresponding to the
    /// call of this function.
    public int setString(Buffer buffer, int offset, String newValue) {
        String oldValue = buffer.getContents().getString(offset);
        BlockId blockId = buffer.getBlockId();
        return SetStringRecord.writeToLog(logManager, transactionNumber, blockId, offset, oldValue, newValue);
    }

    /// Creates an update log for updating a boolean in a given buffer on a given offset.
    ///
    /// @return The log sequence number for the record in the log file corresponding to the
    /// call of this function.
    public int setBoolean(Buffer buffer, int offset, boolean newValue) {
        boolean oldValue = buffer.getContents().getBoolean(offset);
        BlockId blockId = buffer.getBlockId();
        return SetBooleanRecord.writeToLog(logManager, transactionNumber, blockId, offset, oldValue, newValue);
    }

    /// Creates an append block log for a given filename.
    ///
    /// @return The log sequence number for the record in the log file corresponding to the
    /// call of this function.
    public int appendBlock(String filename) {
        return AppendBlockRecord.writeToLog(logManager, transactionNumber, filename);
    }

    /// The algorithm for rolling back a transaction. The transaction that
    /// is being rolled back is the transaction that is tied to the instance
    /// of the recovery manager.
    private void transactionRollback() {
        Iterator<byte[]> iter = logManager.iterator();

        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord record = LogRecord.createLogRecord(bytes);
            if (record.transactionNumber() == transactionNumber) {
                if (record.op() == LogRecordType.START) {
                    // no need to check earlier records because
                    // there will be no records matching this
                    // particular transaction before it was started
                    return;
                }
                record.undo(transaction);
            }
        }
    }

    /// Types of algorithms used for system recovery.
    class RecoveryAlgorithm {
        private final ArrayList<LogRecord> processedRecords = new ArrayList<>();
        private final HashSet<Integer> completedTransactions = new HashSet<>();
        public final boolean undoOnly;

        /// When `undoOnly` is set to `false`, the algorithm
        /// used is the 'Undo-Redo' algorithm.
        public RecoveryAlgorithm(boolean undoOnly) {
            this.undoOnly = undoOnly;
        }

        /// Recover the system if it crashed. The recovery is an idempotent
        /// function so if the system crashes in the middle of recovery,
        /// the system won't be left in an inconsistent state.
        ///
        /// @return The transaction number latest in the log so that new
        /// transactions can start from this one + 1.
        public int systemRecovery() {
            if (undoOnly) {
                return undoOnlyRecover();
            } else {
                return undoRedoRecover();
            }
        }

        /// The 'Undo-Only' algorithm for system recovery. Assumes all commited transactions
        /// will have their changes written out in user data blocks meaning they won't need
        /// to be reapplied on system recovery.
        /// Advantage: faster recovery. Disadvantage: slower commits.
        private int undoOnlyRecover() {
            Iterator<byte[]> iter = logManager.iterator();

            while (iter.hasNext()) {
                byte[] bytes = iter.next();
                LogRecord record = LogRecord.createLogRecord(bytes);
                if (record.op() == LogRecordType.QUIESCENT_CHECKPOINT) {
                    // if a quiescent checkpoint is found, that means the
                    // log doesn't need to be traversed any further because
                    // the database is guaranteed to be in a reasonable state
                    // in correspondence to the logs before the quiescent checkpoint
                    // log
                    return completedTransactions.stream()
                            .max(Integer::compare)
                            .orElse(0);
                }

                if (!undoOnly) {
                    // do not use unnecessary space when doing only
                    // the undo part of the recovery
                    processedRecords.add(record);
                }

                if (record.op() == LogRecordType.COMMIT || record.op() == LogRecordType.ROLLBACK) {
                    // a transaction is completed if it's rollback or commit log
                    // is found in the log file
                    completedTransactions.add(record.transactionNumber());
                } else if (!completedTransactions.contains(record.transactionNumber())) {
                    // if some other than commit or rollback log record type
                    // is encountered, and its corresponding transaction is not
                    // completed (the traversing is done from the end of the file,
                    // so commit and rollback logs will always be encountered first)
                    // it means that record must be undone since a reasonable state is
                    // one where all uncompleted transactions are rolled back
                    record.undo(transaction);
                }
            }

            return completedTransactions.stream()
                    .max(Integer::compare)
                    .orElse(0);
        }

        /// The 'Undo-Redo' algorithm for system recovery.
        /// Disadvantage: slow recovery, uses more memory.
        private int undoRedoRecover() {
            // firstly, call the undo portion of the algorithm
            int biggestTransactionNumber = undoOnlyRecover();

            // redo every record, but in reverse order
            for (LogRecord record : processedRecords.reversed()) {
                if (!completedTransactions.contains(record.transactionNumber())) {
                    // if some other than commit or rollback log record type
                    // is encountered, and its corresponding transaction is
                    // completed (list of completed transactions already exists from
                    // the backward pass in the undo portion) it means that record
                    // must be redone since a reasonable state is one where all
                    // completed transactions are commited
                    record.redo(transaction);
                }
            }

            return biggestTransactionNumber;
        }
    }
}
