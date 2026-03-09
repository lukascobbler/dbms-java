package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.scanDefinitions.UnaryUpdateScan;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;

import java.util.Collection;
import java.util.List;

/// A project scan represents the "projection" relational algebra operator.
/// It is a unary table update scan. The user specifies the list of field
/// names that should be returned by the query.
public class ProjectScan extends UnaryUpdateScan {
    private final Collection<String> fieldList;

    /// A project scan requires the list of field names that should
    /// be visible at the output of this scan and a child scan that
    /// must also be able to update data because a project scan can.
    public ProjectScan(UpdateScan scan, List<String> fieldList) {
        super(scan);
        this.fieldList = fieldList;
    }

    /// The field exists if the user has passed it, not
    /// if it exists in the schema.
    ///
    /// @return True if the field exists, from the list of user-provided field
    /// names.
    @Override
    public boolean hasField(String fieldName) {
        return fieldList.contains(fieldName);
    }
}
