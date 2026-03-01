package com.luka.simpledb.metadataManagement.infoClasses;

/// Holds information about statistics of some table.
public record StatisticsInfo(int numBlocks, int numRecords, UniqueFieldsInfo uniqueFieldsInfo) {
    /// @return The approximated number of unique fields for the given table.
    public int distinctValues(String fieldName) {
        return uniqueFieldsInfo.getUniqueValues(fieldName, numRecords);
    }
}
