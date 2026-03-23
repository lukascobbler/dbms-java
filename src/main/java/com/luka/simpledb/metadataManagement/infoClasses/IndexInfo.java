package com.luka.simpledb.metadataManagement.infoClasses;

import com.luka.simpledb.recordManagement.DatabaseType;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.schema.Schema;
import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;
import com.luka.simpledb.transactionManagement.Transaction;

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
//        switch (indexType) {
//            case HASH -> { return new HashIndex(transaction, indexName, indexLayout); }
//            case B_TREE -> { return new BTreeIndex(transaction, indexName, indexLayout); }
//            default -> throw new IndexNotImplementedException();
//        }
//    }

// todo implement when indexes are done
//    public int blocksAccessed() {
//        int recordsPerBlock = transaction.blockSize() / indexLayout.getSlotSize();
//        int numBlocks = statisticsInfo.numBlocks() / recordsPerBlock;
//        switch (indexType) {
//            case HASH -> { return HashIndex.searchCost(numBlocks, recordsPerBlock); }
//            case B_TREE -> { return BTreeIndex.searchCost(numBlocks, recordsPerBlock); }
//            default -> throw new IndexNotImplementedException();
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
