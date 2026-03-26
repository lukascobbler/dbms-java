package com.luka.lbdb.metadataManagement.infoClasses;

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
    private final Map<String, Byte> booleanFields = new HashMap<>();
    private final Map<String, Integer> nullValueCounter = new HashMap<>();

    /// Add the value to the unique count, for that field name. If the counter
    /// has never seen that field name before, it creates a HyperLogLog for it.
    /// Else, it updates the probabilistic data structure.
    public void addIntValue(String fieldName, int value) {
        HllSketch sketch = sketchesPerField.computeIfAbsent(fieldName,
                k -> new HllSketch(LG_K, TgtHllType.HLL_8));

        sketch.update(value);
    }

    /// Add the value to the unique count, for that field name. If the counter
    /// has never seen that field name before, it creates a HyperLogLog for it.
    /// Else, it updates the probabilistic data structure.
    public void addStringValue(String fieldName, String value) {
        HllSketch sketch = sketchesPerField.computeIfAbsent(fieldName,
                k -> new HllSketch(LG_K, TgtHllType.HLL_8));

        sketch.update(value);
    }

    /// Add the value to the unique count, for that field name. Booleans are special
    /// case because there will only ever be two distinct values, and they are counted
    /// with byte masks.
    public void addBooleanValue(String fieldName, boolean value) {
        byte currentMask = booleanFields.getOrDefault(fieldName, (byte) 0);

        byte seenBit = (byte) (value ? 0b10 : 0b01);

        booleanFields.put(fieldName, (byte) (currentMask | seenBit));
    }

    /// Increase the total number of nulls for a given field.
    public void addNullValue(String fieldName) {
        nullValueCounter.putIfAbsent(fieldName, 0);

        nullValueCounter.put(fieldName, nullValueCounter.get(fieldName) + 1);
    }

    /// @return The approximated number of unique values for a given field,
    /// capped at `numTotalRecords`.
    public int getUniqueValues(String fieldName, int numTotalRecords) {
        if (booleanFields.containsKey(fieldName)) {
            byte mask = booleanFields.get(fieldName);

            int distinctCount = Integer.bitCount(mask & 0xFF);

            return Math.min(distinctCount, numTotalRecords);
        }

        HllSketch sketch = sketchesPerField.get(fieldName);
        if (sketch == null) return 0;

        double estimate = sketch.getEstimate();

        return (int) Math.min(Math.round(estimate), numTotalRecords);
    }

    /// @return The number of null values for a given field, 0 if there is zero
    /// null values for that field.
    public int getNullValues(String fieldName, int numTotalRecords) {
        return Math.min(nullValueCounter.getOrDefault(fieldName, 0), numTotalRecords);
    }
}
