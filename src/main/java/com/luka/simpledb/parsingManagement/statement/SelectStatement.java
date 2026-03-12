package com.luka.simpledb.parsingManagement.statement;

import com.luka.simpledb.parsingManagement.statement.select.SingleSelection;
import java.util.List;

public record SelectStatement(List<SingleSelection> unionizedSelections) implements Statement {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();

        for (SingleSelection singleSelection : unionizedSelections) {
            result.append(singleSelection.toString()).append(" UNION ");
        }

        result.append(';');

        return result.toString();
    }
}
