package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.scanDefinitions.UnaryUpdateScan;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;

import java.util.Collection;
import java.util.List;

public class ProjectScan extends UnaryUpdateScan {
    private final Collection<String> fieldList;

    public ProjectScan(UpdateScan scan, List<String> fieldList) {
        super(scan);
        this.fieldList = fieldList;
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldList.contains(fieldName);
    }
}
