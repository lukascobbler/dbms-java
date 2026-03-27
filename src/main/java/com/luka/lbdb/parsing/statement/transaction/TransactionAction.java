package com.luka.lbdb.parsing.statement.transaction;

import org.jetbrains.annotations.NotNull;

/// Actions on transactions.
public enum TransactionAction {
    START_TRANSACTION,
    COMMIT,
    ROLLBACK;

    @Override
    public @NotNull String toString() {
        return switch (this) {
            case START_TRANSACTION -> "START TRANSACTION";
            case COMMIT -> "COMMIT";
            case ROLLBACK -> "ROLLBACK";
        };
    }
}
