package com.luka.lbdb.db;

import com.luka.lbdb.bufferManagement.BufferManager;
import com.luka.lbdb.fileManagement.FileManager;
import com.luka.lbdb.logManagement.LogManager;
import com.luka.lbdb.metadataManagement.MetadataManager;
import com.luka.lbdb.planning.planner.Planner;
import com.luka.lbdb.records.RecordId;
import com.luka.lbdb.db.settings.LBDBSettings;
import com.luka.lbdb.transactions.Transaction;
import com.luka.lbdb.transactions.TransactionManager;
import com.luka.lbdb.transactions.concurrencyManagement.LockTable;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/// The class that manages the system state. There should be only one per instance
/// of server / embedded connection running.
public class LBDB {
    private final FileManager fileManager;
    private final LogManager logManager;
    private final BufferManager bufferManager;
    private final LockTable lockTable;
    private final MetadataManager metadataManager;
    private final Planner planner;
    private final LBDBSettings settings;
    private final AtomicInteger nextTransactionNum = new AtomicInteger(0);
    private final TransactionManager transactionManager;

    /// Initializes the whole system, given the directory name.
    /// Uses default configuration.
    /// Checks if data already exists at the location:
    /// - if it does, then the recovery algorithm is run;
    /// - if it doesn't, all required files are created and set up for the
    ///  system to run.
    public LBDB(Path dbDirectory) {
        this(dbDirectory, new LBDBSettings());
    }

    /// Initializes the whole system, given the directory name.
    /// Allows custom configuration.
    /// Checks if data already exists at the location:
    /// - if it does, then the recovery algorithm is run;
    /// - if it doesn't, all required files are created and set up for the
    ///  system to run.
    public LBDB(Path dbDirectory, LBDBSettings settings) {
        this.settings = settings;

        fileManager = new FileManager(dbDirectory, settings.BLOCK_SIZE);
        logManager = new LogManager(fileManager, settings.LOG_FILE);
        bufferManager = new BufferManager(fileManager, logManager, settings.BUFFER_POOL_SIZE);
        lockTable = new LockTable();

        Transaction transaction = newTransaction();
        boolean isNew = fileManager.isNew();

        if (!isNew) {
            transaction.recover();
        }

        AtomicInteger nextTableIdNum = new AtomicInteger(0);
        metadataManager = new MetadataManager(transaction, fileManager, nextTableIdNum);

        Map<String, RecordId> lastInsertions = new HashMap<>();
        transactionManager = new TransactionManager(this::newTransaction);
        planner = new Planner(
                settings.getQueryPlanner(metadataManager),
                settings.getUpdatePlanner(metadataManager, lastInsertions),
                transactionManager
        );

        transaction.commit();
    }

    /// Every code run against the database must provide a transaction object.
    ///
    /// @return A new transaction in the system.
    public Transaction newTransaction() {
        return new Transaction(fileManager, logManager, bufferManager, lockTable, settings.UNDO_ONLY_RECOVERY, nextTransactionNum);
    }

    /// @return The metadata manager that is the part of this system object.
    public MetadataManager getMetadataManager() {
        return metadataManager;
    }

    /// @return The planner that is the part of this system object.
    public Planner getPlanner() {
        return planner;
    }

    /// @return The transaction manager that is the part of this system.
    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void shutdown() {
        // todo
    }
}
