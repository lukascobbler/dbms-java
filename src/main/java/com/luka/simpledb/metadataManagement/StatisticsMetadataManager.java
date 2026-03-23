package com.luka.simpledb.metadataManagement;

import com.luka.simpledb.metadataManagement.infoClasses.StatisticsInfo;
import com.luka.simpledb.metadataManagement.infoClasses.UniqueFieldsInfo;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.recordManagement.DatabaseType;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.schema.Schema;
import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;
import com.luka.simpledb.transactionManagement.Transaction;

import java.util.HashMap;
import java.util.Map;

/// Keeps track of useful statistics for each table. They include: number of blocks,
/// number of records, number of unique values for each field. It uses the implementation
/// approach of keeping the statistics in-memory and recalculates them on system startup.
public class StatisticsMetadataManager {
    private final TableMetadataManager tableMetadataManager;
    private Map<String, StatisticsInfo> tableStats;
    private int numCalls;

    /// Initializes the statistics data manager by calculating
    /// statistics for every table.
    public StatisticsMetadataManager(TableMetadataManager tableMetadataManager, Transaction transaction) {
        this.tableMetadataManager = tableMetadataManager;
        refreshStatistics(transaction);
    }

    /// @return Statistical information for a given table. If the stats don't
    /// exist for that table at the time of calling, they are calculated and returned.
    /// Every 100 invocations, the statistics for the whole database get recalculated.
    public synchronized StatisticsInfo getStatisticsInfo(String tableName, Layout layout, Transaction transaction) {
        numCalls++;

        if (numCalls > 100) {
            refreshStatistics(transaction);
        }

        StatisticsInfo statisticsInfo = tableStats.get(tableName);

        if (statisticsInfo == null) {
            statisticsInfo = calculateTableStats(tableName, layout, transaction);
            tableStats.put(tableName, statisticsInfo);
        }

        return statisticsInfo;
    }

    /// Goes over every table and recalculates statistical data for every table.
    /// Synchronized because this should not be happening more than once
    /// at the same time in the whole system.
    private synchronized void refreshStatistics(Transaction transaction) {
        tableStats = new HashMap<>();
        numCalls = 0;

        Layout tableCatalogLayout = tableMetadataManager.getLayout("tablecatalog", transaction);
        TableScan tableCatalogScan = new TableScan(transaction, "tablecatalog", tableCatalogLayout);

        try (tableCatalogScan) {
            while (tableCatalogScan.next()) {
                String tableName = tableCatalogScan.getString("tablename");
                Layout layout = tableMetadataManager.getLayout(tableName, transaction);
                StatisticsInfo statisticsInfo = calculateTableStats(tableName, layout, transaction);
                tableStats.put(tableName, statisticsInfo);
            }
        }
    }

    /// Does a full table scan, incrementing the number of records and
    /// getting the latest block number for every record. Calculates the
    /// approximated number of unique values per field. Null values are skipped,
    /// thus the tables containing them will not have ideal plans, but
    /// they will underestimate instead of overestimate which is generally better.
    /// Synchronized because threads no two threads should be calculating stats for the
    /// same table at the same time.
    ///
    /// @return Calculated statistics for the given table.
    private synchronized StatisticsInfo calculateTableStats(String tableName, Layout layout, Transaction transaction) {
        int numRecords = 0;
        int numBlocks = 0;
        UniqueFieldsInfo uniqueFieldsInfo = new UniqueFieldsInfo();

        TableScan tableScan = new TableScan(transaction, tableName, layout);
        Schema tableSchema = layout.getSchema();

        try (tableScan) {
            while (tableScan.next()) {
                numRecords++;
                numBlocks = tableScan.getRecordId().blockNum() + 1;

                for (String fieldName : tableSchema.getFields()) {
                    if (tableScan.isNull(fieldName)) {
                        continue;
                    }
                    switch (tableSchema.type(fieldName)) {
                        case DatabaseType.INT -> uniqueFieldsInfo.addValue(fieldName, tableScan.getInt(fieldName));
                        case DatabaseType.VARCHAR -> uniqueFieldsInfo.addValue(fieldName, tableScan.getString(fieldName));
                        case DatabaseType.BOOLEAN -> uniqueFieldsInfo.addValue(fieldName, tableScan.getBoolean(fieldName));
                        default -> throw new DatabaseTypeNotImplementedException();
                    }
                }
            }
        }

        return new StatisticsInfo(numBlocks, numRecords, uniqueFieldsInfo);
    }
}
