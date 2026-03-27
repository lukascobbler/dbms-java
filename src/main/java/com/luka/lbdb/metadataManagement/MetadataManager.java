package com.luka.lbdb.metadataManagement;

import com.luka.lbdb.fileManagement.FileManager;
import com.luka.lbdb.metadataManagement.infoClasses.IndexInfo;
import com.luka.lbdb.metadataManagement.infoClasses.IndexType;
import com.luka.lbdb.metadataManagement.infoClasses.StatisticsInfo;
import com.luka.lbdb.metadataManagement.exceptions.*;
import com.luka.lbdb.records.Layout;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.transactionManagement.Transaction;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/// A facade class for every manager. Makes the usage easier even though the
/// code is organized in different files.
public class MetadataManager {
    private final TableMetadataManager tableMetadataManager;
    private final StatisticsMetadataManager statisticsMetadataManager;
    private final IndexMetadataManager indexMetadataManager;

    /// Initializes every type of metadata manager, starting from the table manager.
    public MetadataManager(Transaction transaction, FileManager fileManager, AtomicInteger nextTableNum) {
        boolean isNew = fileManager.lengthInBlocks("tablecatalog.table") == 0;
        tableMetadataManager = new TableMetadataManager(isNew, transaction, nextTableNum);
        statisticsMetadataManager = new StatisticsMetadataManager(tableMetadataManager, transaction);
        indexMetadataManager = new IndexMetadataManager(isNew, tableMetadataManager, statisticsMetadataManager, transaction);
    }

    /// Creates all metadata required for a table, according to the passed schema.
    public void createTable(String tableName, Schema schema, Transaction transaction) {
        tableMetadataManager.createTable(tableName, schema, transaction);
    }

    /// Removes a field from a table and removes an index associated with that field
    /// if it exists. Does not remove data that that field contains or the index files for
    /// that field.
    public void removeField(String tableName, String fieldName, Transaction transaction) {
        tableMetadataManager.removeField(tableName, fieldName, transaction);
        indexMetadataManager.removeIndex(tableName, fieldName, transaction);
    }

    /// @return The layout associated with the passed table name.
    /// @throws TableNotFoundException if the table was not found.
    public Layout getLayout(String tableName, Transaction transaction) {
        return tableMetadataManager.getLayout(tableName, transaction);
    }

    /// Creates all metadata required for an index.
    public void createIndex(String indexName, String tableName, String fieldName,
                            IndexType indexType, Transaction transaction) {
        indexMetadataManager.createIndex(indexName, tableName, fieldName, indexType, transaction);
    }

    /// @return All index information for the passed table. Keys represent field names, and
    /// values represent information for the indexes on those fields.
    public Map<String, IndexInfo> getIndexInfo(String tableName, Transaction transaction) {
        return indexMetadataManager.getIndexInfo(tableName, transaction);
    }

    /// @return Statistical info for the given table. That table's layout is also required.
    public StatisticsInfo getStatisticsInfo(String tableName, Layout layout, Transaction transaction) {
        return statisticsMetadataManager.getStatisticsInfo(tableName, layout, transaction);
    }
}
