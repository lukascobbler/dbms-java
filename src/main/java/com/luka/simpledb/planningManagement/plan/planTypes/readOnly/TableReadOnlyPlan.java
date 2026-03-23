package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.infoClasses.StatisticsInfo;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.schema.Schema;
import com.luka.simpledb.transactionManagement.Transaction;

/// A table plan is a plan that has direct information about a table.
/// It is a special type of plan that isn't related to any relational algebra
/// operator. Read-only operations only.
public class TableReadOnlyPlan implements Plan<Scan> {
    private final Transaction transaction;
    private final String tableName;
    private final Layout tableLayout;
    private final StatisticsInfo statisticsInfo;

    /// Requires the table name, the metadata manager for getting statistical
    /// data and a transaction for which the plan will be opened to a scan.
    /// Note that calculating statistical data may have blocking side effects.
    public TableReadOnlyPlan(Transaction transaction, String tableName, MetadataManager metadataManager) {
        this.transaction = transaction;
        this.tableName = tableName;
        tableLayout = metadataManager.getLayout(tableName, transaction);
        statisticsInfo = metadataManager.getStatisticsInfo(tableName, tableLayout, transaction);
    }

    @Override
    public Scan open() {
        return new TableScan(transaction, tableName, tableLayout);
    }

    /// Table plans know about their number of blocks directly through statistical
    /// information. Note that statistical data isn't 100% up-to-date, thus it is
    /// not 100% accurate.
    ///
    /// @return The number of blocks of the table.
    @Override
    public int blocksAccessed() {
        return statisticsInfo.numBlocks();
    }

    /// Table plans know about their number of records directly through statistical
    /// information. Note that statistical data isn't 100% up-to-date, thus it is
    /// not 100% accurate.
    ///
    /// @return The number of records of the table.
    @Override
    public int recordsOutput() {
        return statisticsInfo.numRecords();
    }

    /// Table plans know about the number of distinct values for a field
    /// directly through statistical information. Note that statistical
    /// data isn't 100% up-to-date, thus it is not 100% accurate.
    ///
    /// @return The number of distinct values for a field.
    @Override
    public int distinctValues(String fieldName) {
        return statisticsInfo.distinctValues(fieldName);
    }

    /// Table plans know about the number null values for a field
    /// directly through statistical information. Note that statistical
    /// data isn't 100% up-to-date, thus it is not 100% accurate.
    ///
    /// @return The number of null values for a field.
    @Override
    public int nullValues(String fieldName) {
        return statisticsInfo.nullValues(fieldName);
    }

    /// @return The table's actual physical schema, containing no additional
    /// fields or modifications.
    @Override
    public Schema outputSchema() {
        return tableLayout.getSchema();
    }
}
