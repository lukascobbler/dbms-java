package com.luka.queryManagement;

import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.NullConstant;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.schema.Schema;
import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.transactionManagement.Transaction;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/// Initialization methods that encapsulate data needed for query tests.
public class QueryTestUtils {

    /// Helper record for test initialization functions.
    public record QueryTestData(SimpleDB db, Transaction tx, List<Layout> layouts) { }

    /// Creates one table "table1", with three fields of each type where
    /// the third field of each type is nullable and set to null.
    public static QueryTestData initializeOneFullTable(Path tmpDir) {
        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("t1_intField1", false);
        sch.addIntField("t1_intField2", false);
        sch.addIntField("t1_intField3", true);
        sch.addStringField("t1_stringField1", 100, false);
        sch.addStringField("t1_stringField2", 100, false);
        sch.addStringField("t1_stringField3", 100, true);
        sch.addBooleanField("t1_boolField1", false);
        sch.addBooleanField("t1_boolField2", false);
        sch.addBooleanField("t1_boolField3", true);

        simpleDB.getMetadataManager().createTable("table1", sch, tx);

        Layout layout = simpleDB.getMetadataManager().getLayout("table1", tx);

        UpdateScan tableScan = new TableScan(tx, "table1", layout);

        try (tableScan) {
            tableScan.beforeFirst();
            for (int i = 0; i < 250; i++) {
                tableScan.insert();
                tableScan.setInt("t1_intField1", i);
                tableScan.setInt("t1_intField2", i + 1);
                tableScan.setValue("t1_intField3", NullConstant.INSTANCE);
                tableScan.setString("t1_stringField1", "str" + i);
                tableScan.setString("t1_stringField2", "str" + i + 1);
                tableScan.setValue("t1_stringField3", NullConstant.INSTANCE);
                tableScan.setBoolean("t1_boolField1", true);
                tableScan.setBoolean("t1_boolField2", false);
                tableScan.setValue("t1_boolField3", NullConstant.INSTANCE);
            }
        }

        return new QueryTestData(simpleDB, tx, Collections.singletonList(layout));
    }

    /// Creates two tables "table1" and "table2", with three fields of each type where
    /// the third field of each type is nullable and set to null.
    public static QueryTestData initializeTwoFullTables(Path tmpDir) {
        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction tx = simpleDB.newTransaction();

        Schema sch1 = new Schema();
        sch1.addIntField("t1_intField1", false);
        sch1.addIntField("t1_intField2", false);
        sch1.addIntField("t1_intField3", true);
        sch1.addStringField("t1_stringField1", 100, false);
        sch1.addStringField("t1_stringField2", 100, false);
        sch1.addStringField("t1_stringField3", 100, true);
        sch1.addBooleanField("t1_boolField1", false);
        sch1.addBooleanField("t1_boolField2", false);
        sch1.addBooleanField("t1_boolField3", true);

        Schema sch2 = new Schema();
        sch2.addIntField("t2_intField1", false);
        sch2.addIntField("t2_intField2", false);
        sch2.addIntField("t2_intField3", true);
        sch2.addStringField("t2_stringField1", 100, false);
        sch2.addStringField("t2_stringField2", 100, false);
        sch2.addStringField("t2_stringField3", 100, true);
        sch2.addBooleanField("t2_boolField1", false);
        sch2.addBooleanField("t2_boolField2", false);
        sch2.addBooleanField("t2_boolField3", true);

        simpleDB.getMetadataManager().createTable("table1", sch1, tx);
        simpleDB.getMetadataManager().createTable("table2", sch2, tx);

        Layout layout1 = simpleDB.getMetadataManager().getLayout("table1", tx);
        Layout layout2 = simpleDB.getMetadataManager().getLayout("table2", tx);

        UpdateScan tableScan1 = new TableScan(tx, "table1", layout1);

        try (tableScan1) {
            tableScan1.beforeFirst();
            for (int i = 0; i < 250; i++) {
                tableScan1.insert();
                tableScan1.setInt("t1_intField1", i);
                tableScan1.setInt("t1_intField2", i + 1);
                tableScan1.setValue("t1_intField3", NullConstant.INSTANCE);
                tableScan1.setString("t1_stringField1", "str" + i);
                tableScan1.setString("t1_stringField2", "str" + i + 1);
                tableScan1.setValue("t1_stringField3", NullConstant.INSTANCE);
                tableScan1.setBoolean("t1_boolField1", true);
                tableScan1.setBoolean("t1_boolField2", false);
                tableScan1.setValue("t1_boolField3", NullConstant.INSTANCE);
            }
        }

        UpdateScan tableScan2 = new TableScan(tx, "table2", layout2);

        try (tableScan2) {
            tableScan2.beforeFirst();
            for (int i = 0; i < 250; i++) {
                tableScan2.insert();
                tableScan2.setInt("t2_intField1", i);
                tableScan2.setInt("t2_intField2", i + 1);
                tableScan2.setValue("t2_intField3", NullConstant.INSTANCE);
                tableScan2.setString("t2_stringField1", "str" + i);
                tableScan2.setString("t2_stringField2", "str" + i + 1);
                tableScan2.setValue("t2_stringField3", NullConstant.INSTANCE);
                tableScan2.setBoolean("t2_boolField1", true);
                tableScan2.setBoolean("t2_boolField2", false);
                tableScan2.setValue("t2_boolField3", NullConstant.INSTANCE);
            }
        }

        return new QueryTestData(simpleDB, tx, Arrays.asList(layout1, layout2));
    }
}
