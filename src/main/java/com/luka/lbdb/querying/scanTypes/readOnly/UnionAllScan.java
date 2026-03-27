package com.luka.lbdb.querying.scanTypes.readOnly;

import com.luka.lbdb.querying.scanDefinitions.BinaryScan;
import com.luka.lbdb.querying.scanDefinitions.Scan;
import com.luka.lbdb.querying.virtualEntities.constant.Constant;

/// A union all scan represents the "union all" relational algebra operator.
/// It is a binary table read-only scan. The user specifies two tables
/// where the result of this scan are the rows from the first table plus
/// the rows from the second table, including duplicated values.
/// Assumes the child scans operate on identical schemas.
public class UnionAllScan extends BinaryScan {
    private boolean isFirstScanSelected = true;

    /// A union scan requires two child scans.
    public UnionAllScan(Scan childScan1, Scan childScan2) {
        super(childScan1, childScan2);
    }

    /// Setting the union scan before the first element requires
    /// setting both child scans to before the first element and
    /// the currently operating scan to the first one.
    @Override
    public void beforeFirst() {
        childScan1.beforeFirst();
        childScan2.beforeFirst();
        isFirstScanSelected = true;
    }

    /// Setting the union scan before the first element requires
    /// setting both child scans to after the last element and
    /// the currently operating scan to the second one.
    @Override
    public void afterLast() {
        childScan1.afterLast();
        childScan2.afterLast();
        isFirstScanSelected = false;
    }

    /// Advancing the union scan to the next record requires checking
    /// that the first scan has more elements, and if it its exhausted
    /// it moves to the iteration of the second scan.
    ///
    /// @return True if there is more records in the union of two scans.
    @Override
    public boolean next() {
        if (isFirstScanSelected) {
            if (childScan1.next()) {
                return true;
            }
            isFirstScanSelected = false;
        }
        return childScan2.next();
    }

    /// Retreating the union scan to the previous record requires checking
    /// that the second scan has more elements, and if it its at its first record
    /// it moves to the iteration of the first scan.
    ///
    /// @return True if the first record of the first scan isn't reached.
    @Override
    public boolean previous() {
        if (!isFirstScanSelected) {
            if (childScan2.previous()) {
                return true;
            }
            isFirstScanSelected = true;
        }
        return childScan1.previous();
    }

    /// Since both scans have the same schema, it is only necessary to check
    /// for either scan having a field.
    ///
    /// @return True if the field exists in the scan union.
    @Override
    public boolean hasField(String fieldName) {
        return childScan1.hasField(fieldName);
    }

    /// @return The constant of the currently positioned scan.
    @Override
    public Constant getValue(String fieldName) {
        if (isFirstScanSelected) {
            return childScan1.getValue(fieldName);
        }

        return childScan2.getValue(fieldName);
    }
}
