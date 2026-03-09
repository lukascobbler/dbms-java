package com.luka.simpledb.queryManagement.scanDefinitions;

import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;

/// A scan is the backbone of how queries interact and get to the
/// data that the user requested. Scans define value getter functions
/// based on field names, as well as navigation methods and how to close
/// the resources when they are not needed anymore.
///
/// At the bottom of the scan abstraction hierarchy is `Scan` which defines
/// read-only value operations and navigation methods on a table or
/// an intermediate result that is treated as a table. All other scans
/// extend this one directly or indirectly. Since every scan will eventually
/// need to read from disk, which is a resource that must be released, an
/// elegant way to do that is by implementing the `AutoCloseable` method
/// that is used with try-with-resources and automatically releases the disk
/// resources.
public abstract class Scan implements AutoCloseable {
    // public API general scan features that concrete scans must implement

    /// Navigates the scan before the first value, so that the API caller
    /// can start iterating from the beginning.
    public abstract void beforeFirst();

    /// Navigates the scan after the last value, so that the API caller
    /// can start iterating after the end.
    public abstract void afterLast();

    /// Advances the scan to the next record.
    ///
    /// @return True if there are more records in the scan, false
    /// if the scan is exhausted.
    public abstract boolean next();

    /// Retreats the scan to the previous record.
    ///
    /// @return True if there are more records in the scan, false
    /// if the scan is at the first record.
    public abstract boolean previous();

    /// @return Whether the given field defined by the field name exists in
    /// the scan.
    public abstract boolean hasField(String fieldName);

    /// An idempotent function that releases scan resources
    /// which can be other scans or disk buffers.
    @Override
    public abstract void close();

    // public API scan getters that concrete scans mustn't redefine

    /// @return The int for the given field name.
    /// @throws FieldNotFoundInScanException if the field doesn't exist for this scan.
    public final int getInt(String fieldName) { validate(fieldName); return internalGetInt(fieldName); }

    /// @return The string for the given field name.
    /// @throws FieldNotFoundInScanException if the field doesn't exist for this scan.
    public final String getString(String fieldName) { validate(fieldName); return internalGetString(fieldName); }

    /// @return The boolean for the given field name.
    /// @throws FieldNotFoundInScanException if the field doesn't exist for this scan.
    public final boolean getBoolean(String fieldName) { validate(fieldName); return internalGetBoolean(fieldName); }

    /// @return The constant value for the given field name.
    /// @throws FieldNotFoundInScanException if the field doesn't exist for this scan.
    public final Constant getValue(String fieldName) { validate(fieldName); return internalGetValue(fieldName); }

    // private API scan getters with no field exist that concrete scans must implement

    /// A direct getter for integer values that does not check
    /// for field name existence. Users do not call this function
    /// directly, they use the public API. It exists to prevent field
    /// checking at every step in the scan hierarchy and is the function
    /// with actual logic for retrieving a value.
    ///
    /// @return The int for the given field name.
    protected abstract int internalGetInt(String fieldName);

    /// A direct getter for string values that does not check
    /// for field name existence. Users do not call this function
    /// directly, they use the public API. It exists to prevent field
    /// checking at every step in the scan hierarchy and is the function
    /// with actual logic for retrieving a value.
    ///
    /// @return The int for the given field name.
    protected abstract String internalGetString(String fieldName);

    /// A direct getter for boolean values that does not check
    /// for field name existence. Users do not call this function
    /// directly, they use the public API. It exists to prevent field
    /// checking at every step in the scan hierarchy and is the function
    /// with actual logic for retrieving a value.
    ///
    /// @return The int for the given field name.
    protected abstract boolean internalGetBoolean(String fieldName);

    /// A direct getter for constants that does not check
    /// for field name existence. Users do not call this function
    /// directly, they use the public API. It exists to prevent field
    /// checking at every step in the scan hierarchy and is the function
    /// with actual logic for retrieving a value.
    ///
    /// @return The int for the given field name.
    protected abstract Constant internalGetValue(String fieldName);

    /// A helper method to disallow any field access without checking for its
    /// existence first.
    protected void validate(String fieldName) {
        if (!hasField(fieldName)) {
            throw new FieldNotFoundInScanException(fieldName);
        }
    }
}
