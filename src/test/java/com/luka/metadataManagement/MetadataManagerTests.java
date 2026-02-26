package com.luka.metadataManagement;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.infoClasses.IndexInfo;
import com.luka.simpledb.metadataManagement.infoClasses.IndexType;
import com.luka.simpledb.metadataManagement.infoClasses.StatisticsInfo;
import com.luka.simpledb.queryManagement.TableScan;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataManagerTests {
    // the test from the book Fig 7.18, modified so
    // asserts are used and prints and randoms removed
    @Test
    public void testMetadataManagerAllTypes() throws IOException {
        String tempDirectory = TestUtils.setUpTempDirectory("temp_metadata_all1");

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
        MetadataManager metadataManager = simpleDB.getMetadataManager();
        Transaction transaction = simpleDB.newTransaction();

        Schema schema = new Schema();
        schema.addIntField("A", false);
        schema.addStringField("B", 9, false);

        metadataManager.createTable("TestTable1", schema, transaction);
        Layout layout = metadataManager.getLayout("TestTable1", transaction);
        int size = layout.getSlotSize();
        // 4 for the slot, 4 for one integer, 9 for the string length, 3 for the
        // bytes per character, 4 for the actual length and 1 for padding to 4
        assertEquals(4 + 4 + ((9 * 3) + 4 + 1), size);
        assertEquals(schema, layout.getSchema());

        TableScan tableScan = new TableScan(transaction, "TestTable1", layout);

        try (tableScan) {
            for (int i = 0; i < 50; i++) {
                tableScan.insert();
                int randomInt = (int) Math.round(Math.random() * 50);
                tableScan.setInt("A", randomInt);
                tableScan.setString("B", "rec" + randomInt);
            }
        }

        StatisticsInfo statisticsInfo = metadataManager.getStatisticsInfo("TestTable1", layout, transaction);
        assertEquals(50, statisticsInfo.numRecords());
        assertEquals(1, statisticsInfo.numBlocks());
        // assertTrue(statisticsInfo.distinctValues("A") > 40); todo
        // assertTrue(statisticsInfo.distinctValues("B") > 40); todo

        String viewDefinition = "select B from MyTable where A = 1";
        metadataManager.createView("TestView1", viewDefinition, transaction);
        assertEquals(viewDefinition, metadataManager.getViewDefinition("TestView1", transaction));

        metadataManager.createIndex("TestIndex1", "TestTable1", "A", IndexType.B_TREE, transaction);
        metadataManager.createIndex("TestIndex2", "TestTable1", "B", IndexType.HASH, transaction);

        Map<String, IndexInfo> indexInfo = metadataManager.getIndexInfo("TestTable1", transaction);
        assertTrue(indexInfo.get("A").recordsOutput() > 50 / 40);
        assertTrue(indexInfo.get("B").recordsOutput() > 50 / 40);

        transaction.commit();
    }
}
