package com.luka.simpledb.metadataManagement.infoClasses;

import com.luka.simpledb.metadataManagement.exceptions.IndexNotImplementedException;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;
import com.luka.simpledb.transactionManagement.Transaction;

import static java.sql.Types.*;

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
            case INTEGER -> schema.addIntField("datavalue", false);
            case BOOLEAN -> schema.addBooleanField("datavalue", false);
            case VARCHAR -> schema.addStringField("datavalue", tableSchema.length(fieldName), false);
            default -> throw new DatabaseTypeNotImplementedException();
        }

        return new Layout(schema, transaction.blockSize());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        IndexInfo indexInfo = (IndexInfo) o;
        return indexName.equals(indexInfo.indexName) && fieldName.equals(indexInfo.fieldName) && indexType == indexInfo.indexType && tableSchema.equals(indexInfo.tableSchema);
    }
}
