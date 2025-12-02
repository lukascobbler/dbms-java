package com.luka.simpledb.transactionManagement.recoveryManagement;

/// Different types of log records that can be found in the log file.
public enum LogRecordType {
    NON_QUIESCENT_CHECKPOINT(0),
    QUIESCENT_CHECKPOINT(1),
    START(2),
    COMMIT(3),
    ROLLBACK(4),
    SETINT(5),
    SETSTRING(6),
    APPEND(7);

    public final int value;

    LogRecordType(int value) {
        this.value = value;
    }

    /// @return The corresponding enum variant of the integer value.
    /// `null` if no enum variant is found for that integer value.
    public static LogRecordType valueOf(int value) {
        for (LogRecordType l : values()) {
            if (java.util.Objects.equals(l.value, value)) {
                return l;
            }
        }

        return null;
    }
}
