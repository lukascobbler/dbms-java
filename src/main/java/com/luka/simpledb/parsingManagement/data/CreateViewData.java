package com.luka.simpledb.parsingManagement.data;

public record CreateViewData(String viewName, QueryData queryData) {
    @Override
    public String toString() {
        return queryData.toString();
    }
}
