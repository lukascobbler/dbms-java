package com.luka.simpledb.metadataManagement.infoClasses;

/// Holds information about statistics of some table.
public record StatisticsInfo(int numBlocks, int numRecords, UniqueFieldsInfo uniqueFieldsInfo) {
    /// @return The approximated number of unique values for the given field and table.
    public int distinctValues(String fieldName) {
        return uniqueFieldsInfo.getUniqueValues(fieldName, numRecords);
    }

    /// @return The approximated number of null values for the given field and table.
    public int nullValues(String fieldName) {
        return uniqueFieldsInfo.getNullValues(fieldName, numRecords);
    }
}
