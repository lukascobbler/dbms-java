package com.luka.simpledb.metadataManagement;

import com.luka.simpledb.metadataManagement.exceptions.IndexDuplicateNameException;
import com.luka.simpledb.metadataManagement.exceptions.IndexTableIncorrectException;
import com.luka.simpledb.metadataManagement.exceptions.TableNotFoundException;
import com.luka.simpledb.metadataManagement.infoClasses.IndexInfo;
import com.luka.simpledb.metadataManagement.infoClasses.IndexType;
import com.luka.simpledb.metadataManagement.infoClasses.StatisticsInfo;
import com.luka.simpledb.queryManagement.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.transactionManagement.Transaction;

import java.util.HashMap;
import java.util.Map;

public class IndexMetadataManager {
    private final Layout indexCatalogLayout;
    private final TableMetadataManager tableMetadataManager;
    private final StatisticsMetadataManager statisticsMetadataManager;

    /// Instantiates a new `IndexManager` that has an always in-memory layout
    /// for the index catalog. If the system is initialized for the first time
    /// (noted by `isNew`), the manager creates catalog tables as well.
    public IndexMetadataManager(boolean isNew, TableMetadataManager tableMetadataManager,
                                StatisticsMetadataManager statisticsMetadataManager,
                                Transaction transaction) {
        this.tableMetadataManager = tableMetadataManager;
        this.statisticsMetadataManager = statisticsMetadataManager;

        Schema schema = new Schema();
        schema.addStringField("indexname", TableMetadataManager.MAX_NAME_LENGTH, false);
        schema.addStringField("tablename", TableMetadataManager.MAX_NAME_LENGTH, false);
        schema.addStringField("fieldname", TableMetadataManager.MAX_NAME_LENGTH, false);
        schema.addStringField("indextype", TableMetadataManager.MAX_NAME_LENGTH, false);
        indexCatalogLayout = new Layout(schema, transaction.blockSize());

        if (isNew) {
            tableMetadataManager.createTable("indexcatalog", schema, transaction);
        }
    }

    /// Creates a new index on a field in a table. Adds the index to the index catalog.
    ///
    /// @throws IndexTableIncorrectException if either the table was not found, or the field in that table
    /// was not found.
    /// @throws IndexDuplicateNameException if an index with the same name already exists.
    public void createIndex(String indexName, String tableName, String fieldName, IndexType indexType, Transaction transaction) {
        try {
            Layout tableLayout = tableMetadataManager.getLayout(tableName, transaction);
            if (!tableLayout.getSchema().hasField(fieldName)) {
                throw new IndexTableIncorrectException();
            }
        } catch (TableNotFoundException _e) {
            throw new IndexTableIncorrectException();
        }

        TableScan indexCatalogDuplicateScan = new TableScan(transaction, "indexcatalog", indexCatalogLayout);

        try (indexCatalogDuplicateScan) {
            while (indexCatalogDuplicateScan.next()) {
                if (indexCatalogDuplicateScan.getString("indexname").equals(indexName)) {
                    throw new IndexDuplicateNameException();
                }
            }
        }

        TableScan indexCatalogScan = new TableScan(transaction, "indexcatalog", indexCatalogLayout);

        try (indexCatalogScan) {
            indexCatalogScan.insert();
            indexCatalogScan.setString("indexname", indexName);
            indexCatalogScan.setString("tablename", tableName);
            indexCatalogScan.setString("fieldname", fieldName);
            indexCatalogScan.setString("indextype", indexType.name());
        }
    }

    /// Removes the metadata for the index given by the table name and field name.
    public void removeIndex(String tableName, String fieldName, Transaction transaction) {
        TableScan indexCatalogScan = new TableScan(transaction, "indexcatalog", indexCatalogLayout);

        try (indexCatalogScan) {
            while (indexCatalogScan.next()) {
                if (
                    indexCatalogScan.getString("tablename").equals(tableName) &&
                    indexCatalogScan.getString("fieldname").equals(fieldName)
                ) {
                    indexCatalogScan.delete();
                    return;
                }
            }
        }
    }

    /// @return The index info for every field of a table name.
    public Map<String, IndexInfo> getIndexInfo(String tableName, Transaction transaction) {
        Map<String, IndexInfo> result = new HashMap<>();

        TableScan indexCatalogScan = new TableScan(transaction, "indexcatalog", indexCatalogLayout);

        try (indexCatalogScan) {
            while (indexCatalogScan.next()) {
                if (!indexCatalogScan.getString("tablename").equals(tableName)) continue;
                String indexName = indexCatalogScan.getString("indexname");
                String fieldName = indexCatalogScan.getString("fieldname");
                String indexType = indexCatalogScan.getString("indextype");
                Layout tableLayout = tableMetadataManager.getLayout(tableName, transaction);
                StatisticsInfo statisticsInfo = statisticsMetadataManager.getStatisticsInfo(tableName, tableLayout, transaction);
                IndexInfo indexInfo = new IndexInfo(indexName, fieldName, IndexType.valueOf(indexType), tableLayout.getSchema(), transaction, statisticsInfo);

                result.put(fieldName, indexInfo);
            }
        }

        return result;
    }
}
