package com.luka.lbdb.metadataManagement;

import com.luka.lbdb.metadataManagement.infoClasses.StatisticsInfo;
import com.luka.lbdb.querying.scanTypes.update.TableScan;
import com.luka.lbdb.querying.virtualEntities.constant.BooleanConstant;
import com.luka.lbdb.querying.virtualEntities.constant.IntConstant;
import com.luka.lbdb.querying.virtualEntities.constant.NullConstant;
import com.luka.lbdb.querying.virtualEntities.constant.StringConstant;
import com.luka.lbdb.records.Layout;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.db.LBDB;
import com.luka.lbdb.transactionManagement.Transaction;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class StatisticsMetadataManagerTests {
    @Test
    public void testCorrectNumberOfRecordsAndBlocks() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        LBDB LBDB = new LBDB(tmpDir);
        Transaction tx = LBDB.getTransactionManager().getOrCreateTransaction(-1);
        MetadataManager metadataManager = LBDB.getMetadataManager();

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
                tableScanInsert.setValue("int1", new IntConstant(100));
                tableScanInsert.setValue("int2", new IntConstant(100));
                tableScanInsert.setValue("int3", new IntConstant(100));
                tableScanInsert.setValue("string1", new StringConstant("string"));
                tableScanInsert.setValue("string2", new StringConstant("string"));
                tableScanInsert.setValue("string3", new StringConstant("string"));
                tableScanInsert.setValue("bool1", new BooleanConstant(true));
                tableScanInsert.setValue("bool2", new BooleanConstant(true));
                tableScanInsert.setValue("bool3", new BooleanConstant(true));
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
        Path tmpDir = TestUtils.setUpTempDirectory();

        LBDB LBDB = new LBDB(tmpDir);
        Transaction tx = LBDB.getTransactionManager().getOrCreateTransaction(-1);
        MetadataManager metadataManager = LBDB.getMetadataManager();

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
                tableScanInsert.setValue("int1", new IntConstant(100));
                tableScanInsert.setValue("int2", new IntConstant(100));
                tableScanInsert.setValue("int3", new IntConstant(100));
                tableScanInsert.setValue("string1", new StringConstant("string"));
                tableScanInsert.setValue("string2", new StringConstant("string"));
                tableScanInsert.setValue("string3", new StringConstant("string"));
                tableScanInsert.setValue("bool1", new BooleanConstant(true));
                tableScanInsert.setValue("bool2", new BooleanConstant(true));
                tableScanInsert.setValue("bool3", new BooleanConstant(true));
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
        Path tmpDir = TestUtils.setUpTempDirectory();

        LBDB LBDB = new LBDB(tmpDir);
        Transaction tx = LBDB.getTransactionManager().getOrCreateTransaction(-1);
        MetadataManager metadataManager = LBDB.getMetadataManager();

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
                tableScanInsert.setValue("int1", new IntConstant(i));
                tableScanInsert.setValue("int2", new IntConstant(i + 1));
                tableScanInsert.setValue("int3", new IntConstant(i + 2));
                tableScanInsert.setValue("string1", new StringConstant(sCache[i]));
                tableScanInsert.setValue("string2", new StringConstant(sCache[i]));
                tableScanInsert.setValue("string3", new StringConstant(sCache[i]));
                tableScanInsert.setValue("bool1", new BooleanConstant(i % 2 == 0));
                tableScanInsert.setValue("bool2", new BooleanConstant(i % 2 == 1));
                tableScanInsert.setValue("bool3", new BooleanConstant(i % 2 == 0));
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

    @Test
    public void testNullValuesCounting() throws Exception {
        Path tmpDir = TestUtils.setUpTempDirectory();

        LBDB LBDB = new LBDB(tmpDir);
        Transaction tx = LBDB.getTransactionManager().getOrCreateTransaction(-1);
        MetadataManager metadataManager = LBDB.getMetadataManager();

        StatisticsMetadataManager sm = (StatisticsMetadataManager)
                TestUtils.getPrivateField(metadataManager, "statisticsMetadataManager");
        Method refreshStatisticsMethod = StatisticsMetadataManager.class.getDeclaredMethod("refreshStatistics", Transaction.class);
        refreshStatisticsMethod.setAccessible(true);

        Schema schema = new Schema();
        schema.addIntField("int1", true);
        schema.addIntField("int2", true);
        schema.addIntField("int3", true);

        metadataManager.createTable("tbl1", schema, tx);
        Layout tableLayout = metadataManager.getLayout("tbl1", tx);

        TableScan tableScanInsert = new TableScan(tx, "tbl1", tableLayout);

        try (tableScanInsert) {
            for (int i = 0; i < 10000; i++) {
                tableScanInsert.insert();
                tableScanInsert.setValue("int1", NullConstant.INSTANCE);
                tableScanInsert.setValue("int2", NullConstant.INSTANCE);
                tableScanInsert.setValue("int3", NullConstant.INSTANCE);
            }
        }

        refreshStatisticsMethod.invoke(sm, tx);

        StatisticsInfo statisticsInfoAfter = metadataManager.getStatisticsInfo("tbl1", tableLayout, tx);

        assertEquals(10000, statisticsInfoAfter.nullValues("int1"));
        assertEquals(10000, statisticsInfoAfter.nullValues("int2"));
        assertEquals(10000, statisticsInfoAfter.nullValues("int3"));
        tx.commit();
    }
}
