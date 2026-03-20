package com.luka.simpledb.planningManagement.plan.planTypes.readOnly;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.infoClasses.StatisticsInfo;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.transactionManagement.Transaction;

public class TableReadOnlyPlan implements Plan<Scan> {
    private final Transaction transaction;
    private final String tableName;
    private final Layout tableLayout;
    private final StatisticsInfo statisticsInfo;

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

    @Override
    public int blocksAccessed() {
        return statisticsInfo.numBlocks();
    }

    @Override
    public int recordsOutput() {
        return statisticsInfo.numRecords();
    }

    @Override
    public int distinctValues(String fieldName) {
        return statisticsInfo.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return tableLayout.getSchema();
    }
}
