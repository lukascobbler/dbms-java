package com.luka.simpledb.planningManagement.plan.planTypes.update;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.infoClasses.StatisticsInfo;
import com.luka.simpledb.planningManagement.plan.Plan;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.schema.Schema;
import com.luka.simpledb.transactionManagement.Transaction;

public class TablePlan implements Plan<UpdateScan> {
    private final Transaction transaction;
    private final String tableName;
    private final Layout tableLayout;
    private final StatisticsInfo statisticsInfo;

    public TablePlan(Transaction transaction, String tableName, MetadataManager metadataManager) {
        this.transaction = transaction;
        this.tableName = tableName;
        tableLayout = metadataManager.getLayout(tableName, transaction);
        statisticsInfo = metadataManager.getStatisticsInfo(tableName, tableLayout, transaction);
    }

    @Override
    public UpdateScan open() {
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
    public Schema outputSchema() {
        return tableLayout.getSchema();
    }
}
