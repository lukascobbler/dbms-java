package com.luka.simpledb.transactionManagement.concurrencyManagement;

import com.luka.simpledb.fileManagement.BlockId;

import java.util.HashMap;
import java.util.Map;

/// Manages locks for blocks for a single transaction.
/// All `ConcurrencyManager` instances for all transactions
/// must share the same lock table because more than one
/// transaction can access the same blocks.
public class ConcurrencyManager {
    private final LockTable lockTable;
    private final Map<BlockId, Character> locks = new HashMap<>();

    /// Initialize concurrency manager with a lock table that all
    /// concurrency managers share.
    public ConcurrencyManager(LockTable lockTable) {
        this.lockTable = lockTable;
    }

    /// Creates a shared lock for the passed block if it
    /// isn't present.
    public void lockShared(BlockId blockId) {
        if (locks.get(blockId) == null) {
            lockTable.lockShared(blockId);
            locks.put(blockId, 'S');
        }
    }

    /// Creates an exclusive lock for the passed block if it
    /// isn't present. However, a shared lock is obtained first
    /// because having an exclusive lock on a block implies
    /// having the shared lock on that same block.
    public void lockExclusive(BlockId blockId) {
        if (!hasExclusiveLock(blockId)) {
            lockShared(blockId);
            lockTable.lockExclusive(blockId);
            locks.put(blockId, 'E');
        }
    }

    /// Releases all locks for this transaction.
    public void release() {
        for (BlockId blockId : locks.keySet()) {
            lockTable.unlock(blockId);
        }
        locks.clear();
    }

    /// @return Whether the block is exclusively locked for this
    /// transaction.
    private boolean hasExclusiveLock(BlockId blockId) {
        return locks.getOrDefault(blockId, 'N').equals('E');
    }
}
