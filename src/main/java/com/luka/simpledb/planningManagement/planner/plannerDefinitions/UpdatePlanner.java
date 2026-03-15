package com.luka.simpledb.planningManagement.planner.plannerDefinitions;

import com.luka.simpledb.parsingManagement.statement.*;
import com.luka.simpledb.transactionManagement.Transaction;

public interface UpdatePlanner {
    int executeInsert(InsertStatement insertStatement, Transaction transaction);
    int executeUpdate(UpdateStatement updateStatement, Transaction transaction);
    int executeDelete(DeleteStatement deleteStatement, Transaction transaction);
    int executeCreateTable(CreateTableStatement createTableStatement, Transaction transaction);
    int executeCreateView(CreateViewStatement createViewStatement, Transaction transaction);
    int executeCreateIndex(CreateIndexStatement createIndexStatement, Transaction transaction);
}
