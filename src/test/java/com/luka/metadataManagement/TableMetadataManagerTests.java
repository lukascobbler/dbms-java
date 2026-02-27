package com.luka.metadataManagement;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.exceptions.TableDuplicateNameException;
import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.queryManagement.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class TableMetadataManagerTests {
    // the test from the book Fig 7.2, modified so
    // asserts are used and prints and randoms removed
    @Test
    public void testTableCreationAndLayoutRetrival() throws IOException {
        String tempDirectory = TestUtils.setUpTempDirectory("temp_metadata_table1");

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        sch.addIntField("A", false);
        sch.addStringField("B", 9, false);
        simpleDB.getMetadataManager().createTable("testTable", sch, tx);

        Layout layout = simpleDB.getMetadataManager().getLayout("testTable", tx);

        assertEquals(sch, layout.getSchema());
        assertEquals(40, layout.getSlotSize());

        tx.commit();
    }

    // the test from the book Fig 7.5
    @Test
    public void testTableCatalog() throws IOException {
        String tempDirectory = TestUtils.setUpTempDirectory("temp_metadata_table2");

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
        Transaction tx = simpleDB.newTransaction();

        Layout layout = simpleDB.getMetadataManager().getLayout("tablecatalog", tx);
        TableScan tableCatalogScan = new TableScan(tx, "tablecatalog", layout);

        try (tableCatalogScan) {
            while (tableCatalogScan.next()) {
                String tableName = tableCatalogScan.getString("tablename");
                int size = tableCatalogScan.getInt("slotsize");
                System.out.println("Table name: " + tableName + ", table size: " + size);
            }
        }

        layout = simpleDB.getMetadataManager().getLayout("fieldcatalog", tx);
        TableScan fieldCatalogScan = new TableScan(tx, "fieldcatalog", layout);

        try (fieldCatalogScan) {
            while (fieldCatalogScan.next()) {
                int tableId = fieldCatalogScan.getInt("tableid");
                String fieldName = fieldCatalogScan.getString("fieldname");
                int type = fieldCatalogScan.getInt("type");
                int length = fieldCatalogScan.getInt("length");
                int offset = fieldCatalogScan.getInt("offset");
                System.out.println("Table Id: " + tableId + ", field name: " +
                        fieldName + ", field type: " + type + ", length: " + length + ", offset: " + offset);
            }
        }

        tx.commit();
    }

    @Test
    public void testDuplicateTableName() throws IOException {
        String tempDirectory = TestUtils.setUpTempDirectory("temp_metadata_table3");

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
        Transaction tx = simpleDB.newTransaction();
        MetadataManager metadataManager = simpleDB.getMetadataManager();

        Schema schema = new Schema();
        schema.addIntField("field", false);

        metadataManager.createTable("tbl1", schema, tx);
        assertThrowsExactly(TableDuplicateNameException.class, () -> metadataManager.createTable("tbl1", schema, tx));

        tx.commit();
    }
}
