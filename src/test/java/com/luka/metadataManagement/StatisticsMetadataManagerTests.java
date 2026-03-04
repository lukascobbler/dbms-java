package com.luka.metadataManagement;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.StatisticsMetadataManager;
import com.luka.simpledb.metadataManagement.infoClasses.StatisticsInfo;
import com.luka.simpledb.queryManagement.scanTypes.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

public class StatisticsMetadataManagerTests {
    @Test
    public void testCorrectNumberOfRecordsAndBlocks() throws Exception {
        String tempDirectory = TestUtils.setUpTempDirectory("temp_metadata_statistics1");

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
        Transaction tx = simpleDB.newTransaction();
        MetadataManager metadataManager = simpleDB.getMetadataManager();

        StatisticsMetadataManager sm = (StatisticsMetadataManager)
                TestUtils.getPrivateField(metadataManager, "statisticsMetadataManager");
        Method refreshStatisticsMethod = StatisticsMetadataManager.class.getDeclaredMethod("refreshStatistics", Transaction.class);
        refreshStatisticsMethod.setAccessible(true);

        Schema schema = new Schema();
        schema.addIntField("int1", false);
        schema.addIntField("int2", false);
        schema.addIntField("int3", false);
        schema.addStringField("string1", 10, false);
        schema.addStringField("string2", 10, false);
        schema.addStringField("string3", 10, false);
        schema.addBooleanField("bool1", false);
        schema.addBooleanField("bool2", false);
        schema.addBooleanField("bool3", false);

        metadataManager.createTable("tbl1", schema, tx);
        Layout tableLayout = metadataManager.getLayout("tbl1", tx);

        refreshStatisticsMethod.invoke(sm, tx);

        StatisticsInfo statisticsInfoBefore = metadataManager.getStatisticsInfo("tbl1", tableLayout, tx);

        assertEquals(0, statisticsInfoBefore.numRecords());
        assertEquals(0, statisticsInfoBefore.numBlocks());

        TableScan tableScanInsert = new TableScan(tx, "tbl1", tableLayout);

        try (tableScanInsert) {
            for (int i = 0; i < 1000; i++) {
                tableScanInsert.insert();
                tableScanInsert.setInt("int1", 100);
                tableScanInsert.setInt("int2", 100);
                tableScanInsert.setInt("int3", 100);
                tableScanInsert.setString("string1", "string");
                tableScanInsert.setString("string2", "string");
                tableScanInsert.setString("string3", "string");
                tableScanInsert.setBoolean("bool1", true);
                tableScanInsert.setBoolean("bool2", true);
                tableScanInsert.setBoolean("bool3", true);
            }
        }

        refreshStatisticsMethod.invoke(sm, tx);

        StatisticsInfo statisticsInfoAfter = metadataManager.getStatisticsInfo("tbl1", tableLayout, tx);

        assertEquals(1000, statisticsInfoAfter.numRecords());
        assertEquals(32, statisticsInfoAfter.numBlocks());

        tx.commit();
    }

    @Test
    public void testCorrectUniqueFieldsSmallCardinality() throws Exception {
        String tempDirectory = TestUtils.setUpTempDirectory("temp_metadata_statistics2");

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
        Transaction tx = simpleDB.newTransaction();
        MetadataManager metadataManager = simpleDB.getMetadataManager();

        StatisticsMetadataManager sm = (StatisticsMetadataManager)
                TestUtils.getPrivateField(metadataManager, "statisticsMetadataManager");
        Method refreshStatisticsMethod = StatisticsMetadataManager.class.getDeclaredMethod("refreshStatistics", Transaction.class);
        refreshStatisticsMethod.setAccessible(true);

        Schema schema = new Schema();
        schema.addIntField("int1", false);
        schema.addIntField("int2", false);
        schema.addIntField("int3", false);
        schema.addStringField("string1", 10, false);
        schema.addStringField("string2", 10, false);
        schema.addStringField("string3", 10, false);
        schema.addBooleanField("bool1", false);
        schema.addBooleanField("bool2", false);
        schema.addBooleanField("bool3", false);

        metadataManager.createTable("tbl1", schema, tx);
        Layout tableLayout = metadataManager.getLayout("tbl1", tx);

        TableScan tableScanInsert = new TableScan(tx, "tbl1", tableLayout);

        try (tableScanInsert) {
            for (int i = 0; i < 1000; i++) {
                tableScanInsert.insert();
                tableScanInsert.setInt("int1", 100);
                tableScanInsert.setInt("int2", 100);
                tableScanInsert.setInt("int3", 100);
                tableScanInsert.setString("string1", "string");
                tableScanInsert.setString("string2", "string");
                tableScanInsert.setString("string3", "string");
                tableScanInsert.setBoolean("bool1", true);
                tableScanInsert.setBoolean("bool2", true);
                tableScanInsert.setBoolean("bool3", true);
            }
        }

        refreshStatisticsMethod.invoke(sm, tx);

        StatisticsInfo statisticsInfoAfter = metadataManager.getStatisticsInfo("tbl1", tableLayout, tx);

        assertEquals(1, statisticsInfoAfter.distinctValues("int1"));
        assertEquals(1, statisticsInfoAfter.distinctValues("int2"));
        assertEquals(1, statisticsInfoAfter.distinctValues("int3"));
        assertEquals(1, statisticsInfoAfter.distinctValues("string1"));
        assertEquals(1, statisticsInfoAfter.distinctValues("string2"));
        assertEquals(1, statisticsInfoAfter.distinctValues("string3"));
        assertEquals(1, statisticsInfoAfter.distinctValues("bool1"));
        assertEquals(1, statisticsInfoAfter.distinctValues("bool2"));
        assertEquals(1, statisticsInfoAfter.distinctValues("bool3"));

        tx.commit();
    }

    @Test
    public void testCorrectUniqueFieldsBigCardinality() throws Exception {
        String tempDirectory = TestUtils.setUpTempDirectory("temp_metadata_statistics3");

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
        Transaction tx = simpleDB.newTransaction();
        MetadataManager metadataManager = simpleDB.getMetadataManager();

        StatisticsMetadataManager sm = (StatisticsMetadataManager)
                TestUtils.getPrivateField(metadataManager, "statisticsMetadataManager");
        Method refreshStatisticsMethod = StatisticsMetadataManager.class.getDeclaredMethod("refreshStatistics", Transaction.class);
        refreshStatisticsMethod.setAccessible(true);

        Schema schema = new Schema();
        schema.addIntField("int1", false);
        schema.addIntField("int2", false);
        schema.addIntField("int3", false);
        schema.addStringField("string1", 10, false);
        schema.addStringField("string2", 10, false);
        schema.addStringField("string3", 10, false);
        schema.addBooleanField("bool1", false);
        schema.addBooleanField("bool2", false);
        schema.addBooleanField("bool3", false);

        metadataManager.createTable("tbl1", schema, tx);
        Layout tableLayout = metadataManager.getLayout("tbl1", tx);

        String[] sCache = new String[10002];
        for (int i = 0; i < sCache.length; i++) {
            sCache[i] = "s" + i;
        }

        TableScan tableScanInsert = new TableScan(tx, "tbl1", tableLayout);

        try (tableScanInsert) {
            for (int i = 0; i < 4000; i++) {
                tableScanInsert.insert();
                tableScanInsert.setInt("int1", i);
                tableScanInsert.setInt("int2", i + 1);
                tableScanInsert.setInt("int3", i + 2);
                tableScanInsert.setString("string1", sCache[i]);
                tableScanInsert.setString("string2", sCache[i]);
                tableScanInsert.setString("string3", sCache[i]);
                tableScanInsert.setBoolean("bool1", i % 2 == 0);
                tableScanInsert.setBoolean("bool2", i % 2 == 1);
                tableScanInsert.setBoolean("bool3", i % 2 == 0);
            }
        }

        refreshStatisticsMethod.invoke(sm, tx);

        StatisticsInfo statisticsInfoAfter = metadataManager.getStatisticsInfo("tbl1", tableLayout, tx);

        assertTrue(statisticsInfoAfter.distinctValues("int1") > 3950);
        assertTrue(statisticsInfoAfter.distinctValues("int2") > 3950);
        assertTrue(statisticsInfoAfter.distinctValues("int3") > 3950);
        assertTrue(statisticsInfoAfter.distinctValues("string1") > 3950);
        assertTrue(statisticsInfoAfter.distinctValues("string2") > 3950);
        assertTrue(statisticsInfoAfter.distinctValues("string3") > 3950);
        assertEquals(2, statisticsInfoAfter.distinctValues("bool1"));
        assertEquals(2, statisticsInfoAfter.distinctValues("bool2"));
        assertEquals(2, statisticsInfoAfter.distinctValues("bool3"));

        assertTrue(statisticsInfoAfter.distinctValues("int1") <= 4000);
        assertTrue(statisticsInfoAfter.distinctValues("int2") <= 4000);
        assertTrue(statisticsInfoAfter.distinctValues("int3") <= 4000);
        assertTrue(statisticsInfoAfter.distinctValues("string1") <= 4000);
        assertTrue(statisticsInfoAfter.distinctValues("string2") <= 4000);
        assertTrue(statisticsInfoAfter.distinctValues("string3") <= 4000);

        tx.commit();
    }
}
