package com.luka.simpledb.queryManagement.scanTypes;

import com.luka.simpledb.queryManagement.scanDefinitions.UnaryScan;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;

import java.util.Collection;
import java.util.List;

public class ProjectScan extends UnaryScan {
    private final Collection<String> fieldList;

    public ProjectScan(Scan scan, List<String> fieldList) {
        super(scan);
        this.fieldList = fieldList;
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldList.contains(fieldName);
    }
}
