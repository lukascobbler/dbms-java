package com.luka.simpledb.transactionManagement.recoveryManagement.logRecordTypes;

import com.luka.simpledb.fileManagement.Page;
import com.luka.simpledb.logManagement.LogManager;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.simpledb.transactionManagement.recoveryManagement.LogRecord;
import com.luka.simpledb.transactionManagement.recoveryManagement.LogRecordType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/// Helper class for representing a non-quiescent checkpoint log record type.
///
/// Structure of the transaction start log record type
/// `<NON_QUIESCENT_CHECKPOINT transactionNumber1 transactionNumber2 transactionNumber3 ...>`
///
/// Example: `<NON_QUIESCENT_CHECKPOINT 0 5 9 7 42 36>`
public class NonQuiescentCheckpointRecord implements LogRecord {
    private final List<Integer> transactionNumbers;

    /// The non-quiescent checkpoint log record type is initialized with a page
    /// of a specific structure defined in the class documentation.
    /// The constructor initializes all values that can be found in
    /// the structure at specific offsets.
    public NonQuiescentCheckpointRecord(Page p) {
        transactionNumbers = new ArrayList<>();

        int offset = Integer.BYTES;
        while (true) {
            try {
                transactionNumbers.add(p.getInt(offset));
            } catch (IndexOutOfBoundsException e) {
                break;
            }

            offset += Integer.BYTES;
        }
    }

    /// Writes out a new non-quiescent checkpoint log type record to the log file.
    /// The structure is the same as it is defined in the class documentation.
    ///
    /// @return The log sequence number representing the newly added
    /// record to the log file.
    public static int writeToLog(LogManager logManager, List<Integer> transactionNumbers) {
        int transactionListSize = transactionNumbers.size() * Integer.BYTES;

        int recordLength = Integer.BYTES + transactionListSize;
        byte[] record = new byte[recordLength];

        // page used for convenience of writing to a byte array
        Page p = new Page(record);
        p.setInt(0, LogRecordType.NON_QUIESCENT_CHECKPOINT.value);

        int offset = Integer.BYTES;
        while (offset < recordLength) {
            int transactionNumberAtOffset = offset / Integer.BYTES;
            p.setInt(offset, transactionNumbers.get(transactionNumberAtOffset));

            offset += Integer.BYTES;
        }

        return logManager.append(record);
    }

    /// @return The `NON_QUIESCENT_CHECKPOINT` log record type.
    @Override
    public LogRecordType op() {
        return LogRecordType.NON_QUIESCENT_CHECKPOINT;
    }

    /// Non-quiescent checkpoints have multiple transactions related to them so no
    /// transaction number can be returned as the single one.
    ///
    /// @return -1 as the invalid transaction number.
    @Override
    public int transactionNumber() {
        return -1;
    }

    /// Does nothing as this is not an update record type.
    @Override
    public void undo(Transaction transaction) { }

    @Override
    public String toString() {
        return "<" + LogRecordType.NON_QUIESCENT_CHECKPOINT +
                transactionNumbers.stream().map(String::valueOf)
                        .collect(Collectors.joining(" ")) + ">";
    }
}
