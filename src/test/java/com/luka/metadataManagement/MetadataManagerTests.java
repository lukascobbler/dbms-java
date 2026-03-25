package com.luka.metadataManagement;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.StatisticsMetadataManager;
import com.luka.simpledb.metadataManagement.TableMetadataManager;
import com.luka.simpledb.metadataManagement.infoClasses.IndexInfo;
import com.luka.simpledb.metadataManagement.infoClasses.IndexType;
import com.luka.simpledb.metadataManagement.infoClasses.StatisticsInfo;
import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.constant.StringConstant;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.schema.Schema;
import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MetadataManagerTests {
    // the test from the book Fig 7.18, modified so
    // asserts are used and prints and randoms removed
    @Test
    public void testMetadataManagerAllTypes() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);
        MetadataManager metadataManager = simpleDB.getMetadataManager();
        Transaction transaction = simpleDB.newTransaction();

        StatisticsMetadataManager sm = (StatisticsMetadataManager)
                TestUtils.getPrivateField(metadataManager, "statisticsMetadataManager");
        Method refreshStatisticsMethod = StatisticsMetadataManager.class.getDeclaredMethod("refreshStatistics", Transaction.class);
        refreshStatisticsMethod.setAccessible(true);

        Schema schema = new Schema();
        schema.addIntField("A", false);
        schema.addStringField("B", 9, false);

        metadataManager.createTable("TestTable1", schema, transaction);
        Layout layout = metadataManager.getLayout("TestTable1", transaction);
        int size = layout.recordLength();
        // 4 for the slot, 4 for one integer, 9 for the string runtimeLength, 3 for the
        // bytes per character, 4 for the actual runtimeLength and 1 for padding to 4
        assertEquals(4 + 4 + ((9 * 3) + 4 + 1), size);
        assertEquals(schema, layout.getSchema());

        TableScan tableScan = new TableScan(transaction, "TestTable1", layout);

        try (tableScan) {
            for (int i = 0; i < 50; i++) {
                tableScan.insert();
                tableScan.setValue("A", new IntConstant(i));
                tableScan.setValue("B", new StringConstant("rec" + i));
            }
        }

        refreshStatisticsMethod.invoke(sm, transaction);

        StatisticsInfo statisticsInfo = metadataManager.getStatisticsInfo("TestTable1", layout, transaction);
        assertEquals(50, statisticsInfo.numRecords());
        assertEquals(1, statisticsInfo.numBlocks());
        assertTrue(statisticsInfo.distinctValues("A") > 40);
        assertTrue(statisticsInfo.distinctValues("B") > 40);

        metadataManager.createIndex("TestIndex1", "TestTable1", "A", IndexType.BTREE, transaction);
        metadataManager.createIndex("TestIndex2", "TestTable1", "B", IndexType.HASH, transaction);

        Map<String, IndexInfo> indexInfo = metadataManager.getIndexInfo("TestTable1", transaction);
        assertTrue(indexInfo.get("A").recordsOutput() >= 50 / 40);
        assertTrue(indexInfo.get("B").recordsOutput() >= 50 / 40);

        transaction.commit();
    }

    @Test
    public void testRemoveFieldReconstructTableAndIndex() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction transaction = simpleDB.newTransaction();
        MetadataManager metadataManager = simpleDB.getMetadataManager();

        Schema schema = new Schema();
        schema.addIntField("not-removed1", false);
        schema.addIntField("removed1", false);
        schema.addIntField("not-removed2", false);
        schema.addIntField("removed2", false);
        schema.addIntField("not-removed3", false);

        metadataManager.createTable("TestTable1", schema, transaction);
        metadataManager.createIndex("idx1", "TestTable1", "not-removed1", IndexType.HASH, transaction);
        metadataManager.createIndex("idx2", "TestTable1", "not-removed2", IndexType.HASH, transaction);
        metadataManager.createIndex("idx3", "TestTable1", "not-removed3", IndexType.HASH, transaction);
        metadataManager.createIndex("idx4", "TestTable1", "removed1", IndexType.HASH, transaction);
        metadataManager.createIndex("idx5", "TestTable1", "removed2", IndexType.HASH, transaction);
        Layout layout = metadataManager.getLayout("TestTable1", transaction);

        TableScan tableScanInsert = new TableScan(transaction, "TestTable1", layout);

        try (tableScanInsert) {
            for (int i = 0; i < 1000; i++) {
                tableScanInsert.insert();
                tableScanInsert.setValue("not-removed1", new IntConstant(100));
                tableScanInsert.setValue("removed1", new IntConstant(100));
                tableScanInsert.setValue("not-removed2", new IntConstant(100));
                tableScanInsert.setValue("removed2", new IntConstant(100));
                tableScanInsert.setValue("not-removed3", new IntConstant(100));
            }
        }

        metadataManager.removeField("TestTable1", "removed1", transaction);
        metadataManager.removeField("TestTable1", "removed2", transaction);

        layout = metadataManager.getLayout("TestTable1", transaction);
        TableScan tableScanGet = new TableScan(transaction, "TestTable1", layout);

        try (tableScanGet) {
            tableScanGet.beforeFirst();
            while (tableScanGet.next()) {
                assertEquals(100, tableScanGet.getValue("not-removed1").asInt());
                assertEquals(100, tableScanGet.getValue("not-removed2").asInt());
                assertEquals(100, tableScanGet.getValue("not-removed3").asInt());
                assertFalse(tableScanGet.hasField("removed1"));
                assertFalse(tableScanGet.hasField("removed2"));
                assertThrowsExactly(FieldNotFoundInScanException.class, () -> tableScanGet.getValue("removed1").asInt());
                assertThrowsExactly(FieldNotFoundInScanException.class, () -> tableScanGet.getValue("removed2").asInt());
            }
        }

        Map<String, IndexInfo> indexes = metadataManager.getIndexInfo("TestTable1", transaction);
        assertFalse(indexes.containsKey("removed1"));
        assertFalse(indexes.containsKey("removed2"));

        TableMetadataManager tableMetadataManager = (TableMetadataManager) TestUtils.getPrivateField(metadataManager, "tableMetadataManager");
        Layout fieldCatalogLayout = (Layout) TestUtils.getPrivateField(tableMetadataManager, "fieldCatalogLayout");
        TableScan fieldCatalogScan = new TableScan(transaction, "fieldcatalog", fieldCatalogLayout);

        boolean removed1Found = false, removed2Found = false, removed3Found = false;
        try (fieldCatalogScan) {
            while (fieldCatalogScan.next()) {
                if (fieldCatalogScan.getValue("fieldname").asString().equals("removed1")) {
                    removed1Found = true;
                }
                if (fieldCatalogScan.getValue("fieldname").asString().equals("removed2")) {
                    removed2Found = true;
                }
                if (fieldCatalogScan.getValue("fieldname").asString().equals("removed3")) {
                    removed3Found = true;
                }
            }
        }

        assertFalse(removed1Found);
        assertFalse(removed2Found);
        assertFalse(removed3Found);

        transaction.commit();
    }
}
