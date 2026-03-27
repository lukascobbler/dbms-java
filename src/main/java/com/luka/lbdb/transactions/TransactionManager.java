package com.luka.lbdb.transactions;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/// Maps different sessions to their transactions, for persistence across
/// multiple commands.
public class TransactionManager {
    private final Map<Long, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private final Supplier<Transaction> transactionSupplier;
    private boolean acceptingNewTransactions = true;
    private boolean writingCheckpoint = false;

    /// For the transaction manager to work, there must be a way of creating
    /// new transactions, which is the transaction supplier.
    public TransactionManager(Supplier<Transaction> transactionSupplier) {
        this.transactionSupplier = transactionSupplier;
    }

    /// @return A new transaction object that isn't managed by this
    /// manager, meaning it must be commited or rolled back by the
    /// API caller.
    public Transaction getAutoCommitTransaction() {
        return transactionSupplier.get();
    }

    /// @return `Optional.some()` if the provided session id's transaction
    /// is managed.
    public Optional<Transaction> getManualCommitTransaction(long sessionId) {
        return Optional.ofNullable(activeTransactions.get(sessionId));
        // todo should transaction implement auto closeable (if yes, should the close rollback or commit)
    }

    /// A new managed transaction will be put in the system for the provided
    /// session id, and from this call onwards, the session will be tied to
    /// that transaction until it is commited or rolled back.
    public void putNewManualTransaction(long sessionId) {
        activeTransactions.put(sessionId, transactionSupplier.get());
    }

    /// Commits a managed transaction for the provided session id.
    ///
    /// @return False if no managed transaction is found for the
    /// provided session id.
    public boolean commitManualTransaction(long sessionId) {
        Transaction tx = activeTransactions.remove(sessionId);
        if (tx != null) tx.commit();
        return tx != null;
    }

    /// Rolls back a managed transaction for the provided session id.
    ///
    /// @return False if no managed transaction is found for the
    /// provided session id.
    public boolean rollbackManualTransaction(long sessionId) {
        Transaction tx = activeTransactions.remove(sessionId);
        if (tx != null) tx.rollback();
        return tx != null;
    }

    public void shutdown() {

    }

    public synchronized void writeCheckpoint() {

    }
}
