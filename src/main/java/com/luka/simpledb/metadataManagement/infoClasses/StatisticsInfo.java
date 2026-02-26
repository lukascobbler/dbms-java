package com.luka.simpledb.metadataManagement.infoClasses;

public record StatisticsInfo(int numBlocks, int numRecords) {
    public int distinctValues(String fieldName) {
        return 1 + numRecords / 3; // todo
    }
}
