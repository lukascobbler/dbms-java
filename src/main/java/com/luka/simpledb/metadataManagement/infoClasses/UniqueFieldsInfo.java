package com.luka.simpledb.metadataManagement.infoClasses;

import com.luka.simpledb.recordManagement.exceptions.DatabaseTypeNotImplementedException;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;

import java.util.*;

/// Keeps track of unique values per field. Internally, it uses a
/// HyperLogLog algorithm from the Apache DataSketches library.
/// It is a very accurate probabilistic data structure
/// for millions of unique values, and doesn't store any of them.
public class UniqueFieldsInfo {
    private static final int LG_K = 16;
    private final Map<String, HllSketch> sketchesPerField = new HashMap<>();

    /// Add the value to the unique count, for that field name. If the counter
    /// has never seen that field name before, it creates a HyperLogLog for it.
    /// Else, it updates the probabilistic data structure.
    ///
    /// @throws DatabaseTypeNotImplementedException if the Java type of the value
    /// isn't supported by the database.
    public void addValue(String fieldName, Object value) {
        if (value == null) return;

        HllSketch sketch = sketchesPerField.computeIfAbsent(fieldName,
                k -> new HllSketch(LG_K, TgtHllType.HLL_8));

        switch (value) {
            case Integer i -> sketch.update(i);
            case String s -> sketch.update(s);
            case Boolean b -> sketch.update(b ? 1 : 0);
            default -> throw new DatabaseTypeNotImplementedException();
        }
    }

    /// @return The approximated number of unique values for a given field,
    /// capped at `numTotalRecords`.
    public int getUniqueValues(String fieldName, int numTotalRecords) {
        HllSketch sketch = sketchesPerField.get(fieldName);
        if (sketch == null) return 0;

        double estimate = sketch.getEstimate();

        return (int) Math.min(Math.round(estimate), numTotalRecords);
    }
}
