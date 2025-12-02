package com.luka.simpledb.transactionManagement.concurrencyManagement;

import com.luka.simpledb.fileManagement.BlockId;
import com.luka.simpledb.transactionManagement.concurrencyManagement.exceptions.LockAbortException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// A `LockTable` is responsible for knowing how many
/// shared locks exist for a given block or if that block
/// is exclusively locked. One `LockTable` instance should
/// exist in the whole system because blocks' lock information
/// is shared across every transaction.
public class LockTable {
    public static final long MAX_TIME = 10_000;
    private final Map<BlockId, Object> blockIdConcurrencyLocks = new ConcurrentHashMap<>();
    private final Map<BlockId, Integer> perBlockIdLockValues = new ConcurrentHashMap<>();

    /// Tries to create a shared lock for the passed block. If
    /// the block already has a shared lock, increments the number
    /// of shared locks for that block by one.
    /// Waits until the exclusive lock has been lifted or a
    /// maximal time period of `MAX_TIME`.
    ///
    /// @throws LockAbortException if the block wasn't successfully
    /// locked.
    public void lockShared(BlockId blockId) {
        long timestamp = System.currentTimeMillis();

        Object lock = getLock(blockId);

        synchronized (lock) {
            try {
                while (hasExclusiveLock(blockId) && !waitingTooLong(timestamp)) {
                    lock.wait(MAX_TIME);
                }
                if (hasExclusiveLock(blockId)) {
                    throw new LockAbortException();
                }
                int currentLockValue = getLockValue(blockId);
                perBlockIdLockValues.put(blockId, currentLockValue + 1);
            } catch (InterruptedException e) {
                throw new LockAbortException();
            }
        }
    }

    /// Tries to create an exclusive lock for the passed block.
    /// Waits until all shared locks have been lifted or a
    /// maximal time period of `MAX_TIME`.
    ///
    /// @throws LockAbortException if the block wasn't successfully
    /// locked.
    public void lockExclusive(BlockId blockId) {
        long timestamp = System.currentTimeMillis();

        Object lock = getLock(blockId);

        synchronized (lock) {
            try {
                while (hasOtherSharedLocks(blockId) && !waitingTooLong(timestamp)) {
                    lock.wait(MAX_TIME);
                }
                if (hasOtherSharedLocks(blockId)) {
                    throw new LockAbortException();
                }
                perBlockIdLockValues.put(blockId, -1);
            } catch (InterruptedException e) {
                throw new LockAbortException();
            }
        }
    }

    /// If the passed block has more than one shared lock
    /// assigned to it, calling `unlock` once decrements the
    /// number by one. Else, if the passed block has one shared
    /// lock or an exclusive lock assigned to it, calling 'unlock'
    /// removes the lock entirely. When the lock is removed entirely,
    /// all threads waiting on all locks are notified.
    public void unlock(BlockId blockId) {
        Object lock = getLock(blockId);

        synchronized (lock) {
            int currentLockValue = getLockValue(blockId);
            if (currentLockValue > 1) {
                perBlockIdLockValues.put(blockId, currentLockValue - 1);
            } else {
                perBlockIdLockValues.remove(blockId);
                lock.notifyAll();
            }
        }
    }

    /// A block has an exclusive lock assigned to it if
    /// the map contains a constant of `-1`.
    ///
    /// @return Whether the block has an exclusive lock assigned to it.
    private boolean hasExclusiveLock(BlockId blockId) {
        return getLockValue(blockId) == -1;
    }

    /// A block has shared locks assigned to it if
    /// the map contains a number greater than 0.
    ///
    /// @return Whether the block has more than one shared lock assigned to it.
    private boolean hasOtherSharedLocks(BlockId blockId) {
        return getLockValue(blockId) > 1;
    }

    /// Checks if the system has waited too long for a lock
    /// (or a number of locks) to be lifted.
    ///
    /// @return Whether the thread has waited for too long for lock lifting.
    private boolean waitingTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }

    /// @return The value which represents either the number
    /// of shared locks if it is greater than 0, or an exclusive
    /// lock if it is equal to -1.
    private int getLockValue(BlockId blockId) {
        return perBlockIdLockValues.getOrDefault(blockId, 0);
    }

    /// If the block id doesn't exist in the concurrency lock table, add it.
    ///
    /// @return The lock for the specified block id.
    private Object getLock(BlockId blockId) {
        return blockIdConcurrencyLocks.computeIfAbsent(blockId, b -> new Object());
    }
}
