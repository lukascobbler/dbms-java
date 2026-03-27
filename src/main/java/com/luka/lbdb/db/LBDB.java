package com.luka.lbdb.db;

import com.luka.lbdb.fileManagement.FileManager;
import com.luka.lbdb.metadataManagement.MetadataManager;
import com.luka.lbdb.planning.planner.Planner;
import com.luka.lbdb.records.RecordId;
import com.luka.lbdb.db.settings.LBDBSettings;
import com.luka.lbdb.transactionManagement.Transaction;
import com.luka.lbdb.transactionManagement.TransactionManager;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/// The class that manages the system state. There should be only one per instance
/// of server / embedded connection running.
public class LBDB {
    private final MetadataManager metadataManager;
    private final Planner planner;
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
        FileManager fileManager = new FileManager(dbDirectory, settings.BLOCK_SIZE);
        boolean isNew = fileManager.isNew();

        transactionManager = new TransactionManager(fileManager, settings);

        Transaction transaction = transactionManager.getOrCreateTransaction(-1);

        if (!isNew) {
            transaction.recover();
        }

        AtomicInteger nextTableIdNum = new AtomicInteger(0);
        metadataManager = new MetadataManager(transaction, fileManager, nextTableIdNum);

        transaction.commit();

        Map<String, RecordId> lastInsertions = new HashMap<>();
        planner = new Planner(
                settings.getQueryPlanner(metadataManager),
                settings.getUpdatePlanner(metadataManager, lastInsertions),
                transactionManager
        );
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
}
