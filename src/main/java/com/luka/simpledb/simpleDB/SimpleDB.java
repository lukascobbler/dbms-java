package com.luka.simpledb.simpleDB;

import com.luka.simpledb.bufferManagement.BufferManager;
import com.luka.simpledb.fileManagement.FileManager;
import com.luka.simpledb.logManagement.LogManager;
import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.simpledb.transactionManagement.concurrencyManagement.LockTable;

import java.io.File;

/// The class that manages the system state. There should be only one per instance
/// of server / embedded connection running.
public class SimpleDB {
    private final FileManager fileManager;
    private final LogManager logManager;
    private final BufferManager bufferManager;
    private final LockTable lockTable;
    private final MetadataManager metadataManager;
    private final SimpleDBSettings settings;

    /// Initializes the whole system, given the directory name.
    /// Uses default configuration.
    /// Checks if data already exists at the location:
    /// - if it does, then the recovery algorithm is run;
    /// - if it doesn't, all required files are created and set up for the
    ///  system to run.
    public SimpleDB(String directoryName) {
        this(directoryName, new SimpleDBSettings());
    }

    /// Initializes the whole system, given the directory name.
    /// Allows custom configuration.
    /// Checks if data already exists at the location:
    /// - if it does, then the recovery algorithm is run;
    /// - if it doesn't, all required files are created and set up for the
    ///  system to run.
    public SimpleDB(String directoryName, SimpleDBSettings settings) {
        this.settings = settings;
        File dbDirectory = new File(directoryName);

        fileManager = new FileManager(dbDirectory, settings.BLOCK_SIZE);
        logManager = new LogManager(fileManager, settings.LOG_FILE);
        bufferManager = new BufferManager(fileManager, logManager, settings.BUFFER_SIZE);
        lockTable = new LockTable();

        Transaction transaction = newTransaction();
        boolean isNew = fileManager.isNew();

        if (!isNew) {
            transaction.recover();
        }

        metadataManager = new MetadataManager(transaction, fileManager);
        transaction.commit();
    }

    /// Every code run against the database must provide a transaction object.
    ///
    /// @return A new transaction in the system.
    public Transaction newTransaction() {
        return new Transaction(fileManager, logManager, bufferManager, lockTable, settings.UNDO_ONLY_RECOVERY);
    }

    /// @return The metadata manager that is the part of this system object.
    public MetadataManager getMetadataManager() {
        return metadataManager;
    }
}
