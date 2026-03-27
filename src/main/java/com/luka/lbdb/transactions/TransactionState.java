package com.luka.lbdb.transactions;

/// Different states a transaction can be in.
public enum TransactionState {
    IN_PROGRESS,
    COMMITED,
    ROLLED_BACK
}
