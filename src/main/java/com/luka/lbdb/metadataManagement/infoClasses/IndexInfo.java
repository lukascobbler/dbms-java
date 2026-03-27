package com.luka.lbdb.metadataManagement.infoClasses;

import com.luka.lbdb.records.DatabaseType;
import com.luka.lbdb.records.Layout;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.records.exceptions.DatabaseTypeNotImplementedException;
import com.luka.lbdb.transactionManagement.Transaction;

// todo comment class once indexes are implemented
public class IndexInfo {
    private final String indexName, fieldName;
    private final IndexType indexType;
    private final Transaction transaction;
    private final Schema tableSchema;
    private final Layout indexLayout;
    private final StatisticsInfo statisticsInfo;

    public IndexInfo(String indexName, String fieldName, IndexType indexType, Schema tableSchema,
                     Transaction transaction, StatisticsInfo statisticsInfo) {
        this.indexName = indexName;
        this.fieldName = fieldName;
        this.indexType = indexType;
        this.transaction = transaction;
        this.statisticsInfo = statisticsInfo;
        this.tableSchema = tableSchema;
        this.indexLayout = createIndexLayout();
    }

// todo implement when indexes are done
//    public Index open() {
//        return switch (indexType) {
//            case HASH -> { new HashIndex(transaction, indexName, indexLayout); }
//            case BTREE -> { new BTreeIndex(transaction, indexName, indexLayout); }
//        };
//    }

// todo implement when indexes are done
//    public int blocksAccessed() {
//        int recordsPerBlock = transaction.blockSize() / indexLayout.getSlotSize();
//        int numBlocks = statisticsInfo.numBlocks() / recordsPerBlock;
//        return switch (indexType) {
//            case HASH -> { HashIndex.searchCost(numBlocks, recordsPerBlock); }
//            case B_TREE -> { BTreeIndex.searchCost(numBlocks, recordsPerBlock); }
//        }
//    }

    public int recordsOutput() {
        return statisticsInfo.numRecords() / statisticsInfo.distinctValues(fieldName);
    }

    public int distinctValues(String fieldName) {
        return fieldName.equals(this.fieldName) ? 1 : statisticsInfo.distinctValues(fieldName);
    }

    private Layout createIndexLayout() {
        Schema schema = new Schema();
        schema.addIntField("block", false);
        schema.addIntField("id", false);

        switch (tableSchema.type(fieldName)) {
            case DatabaseType.INT -> schema.addIntField("datavalue", false);
            case DatabaseType.BOOLEAN -> schema.addBooleanField("datavalue", false);
            case DatabaseType.VARCHAR -> schema.addStringField(
                    "datavalue", tableSchema.runtimeLength(fieldName), false);
            default -> throw new DatabaseTypeNotImplementedException();
        }

        return new Layout(schema, transaction.blockSize());
    }
}
