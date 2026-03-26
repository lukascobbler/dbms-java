package com.luka.lbdb.metadataManagement;

import com.luka.lbdb.metadataManagement.exceptions.IndexAlreadyExistsException;
import com.luka.lbdb.metadataManagement.exceptions.IndexDuplicateNameException;
import com.luka.lbdb.metadataManagement.exceptions.IndexTableIncorrectException;
import com.luka.lbdb.metadataManagement.exceptions.TableNotFoundException;
import com.luka.lbdb.metadataManagement.infoClasses.IndexInfo;
import com.luka.lbdb.metadataManagement.infoClasses.IndexType;
import com.luka.lbdb.metadataManagement.infoClasses.StatisticsInfo;
import com.luka.lbdb.querying.scanTypes.update.TableScan;
import com.luka.lbdb.querying.virtualEntities.constant.StringConstant;
import com.luka.lbdb.records.Layout;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.transactions.Transaction;

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
    /// @throws TableNotFoundException if the table was not found.
    /// @throws IndexTableIncorrectException if the field in the table was not found.
    /// @throws IndexDuplicateNameException if an index with the same name already exists.
    /// @throws IndexAlreadyExistsException if an index already exists for a given fieldName on
    /// the table.
    public void createIndex(String indexName, String tableName, String fieldName, IndexType indexType, Transaction transaction) {
        Layout tableLayout = tableMetadataManager.getLayout(tableName, transaction);
        if (!tableLayout.getSchema().hasField(fieldName)) {
            throw new IndexTableIncorrectException();
        }

        TableScan indexCatalogDuplicateScan = new TableScan(transaction, "indexcatalog", indexCatalogLayout);

        boolean indexNameDuplicate = false;
        boolean indexTableFieldDuplicate = false;

        try (indexCatalogDuplicateScan) {
            while (indexCatalogDuplicateScan.next()) {
                if (indexCatalogDuplicateScan.getValue("indexname").asString().equals(indexName)) {
                    indexNameDuplicate = true;
                }
                if (indexCatalogDuplicateScan.getValue("tablename").asString().equals(tableName) &&
                    indexCatalogDuplicateScan.getValue("fieldname").asString().equals(fieldName)) {
                    indexTableFieldDuplicate = true;
                }
            }
        }

        if (indexNameDuplicate) throw new IndexDuplicateNameException();
        if (indexTableFieldDuplicate) throw new IndexAlreadyExistsException();

        TableScan indexCatalogScan = new TableScan(transaction, "indexcatalog", indexCatalogLayout);

        try (indexCatalogScan) {
            indexCatalogScan.insert();
            indexCatalogScan.setValue("indexname", new StringConstant(indexName));
            indexCatalogScan.setValue("tablename", new StringConstant(tableName));
            indexCatalogScan.setValue("fieldname", new StringConstant(fieldName));
            indexCatalogScan.setValue("indextype", new StringConstant(indexType.name()));
        }
    }

    /// Removes the metadata for the index given by the table name and field name.
    public void removeIndex(String tableName, String fieldName, Transaction transaction) {
        TableScan indexCatalogScan = new TableScan(transaction, "indexcatalog", indexCatalogLayout);

        try (indexCatalogScan) {
            while (indexCatalogScan.next()) {
                if (
                    indexCatalogScan.getValue("tablename").asString().equals(tableName) &&
                    indexCatalogScan.getValue("fieldname").asString().equals(fieldName)
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
                if (!indexCatalogScan.getValue("tablename").asString().equals(tableName)) continue;
                String indexName = indexCatalogScan.getValue("indexname").asString();
                String fieldName = indexCatalogScan.getValue("fieldname").asString();
                String indexType = indexCatalogScan.getValue("indextype").asString();
                Layout tableLayout = tableMetadataManager.getLayout(tableName, transaction);
                StatisticsInfo statisticsInfo = statisticsMetadataManager.getStatisticsInfo(tableName, tableLayout, transaction);
                IndexInfo indexInfo = new IndexInfo(indexName, fieldName, IndexType.valueOf(indexType), tableLayout.getSchema(), transaction, statisticsInfo);

                result.put(fieldName, indexInfo);
            }
        }

        return result;
    }
}
