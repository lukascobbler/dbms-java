package com.luka.metadataManagement;

import com.luka.simpledb.metadataManagement.MetadataManager;
import com.luka.simpledb.metadataManagement.exceptions.IndexDuplicateNameException;
import com.luka.simpledb.metadataManagement.exceptions.IndexTableIncorrectException;
import com.luka.simpledb.metadataManagement.infoClasses.IndexType;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class IndexMetadataManagerTests {
    @Test
    public void testDuplicateIndex() throws IOException {
        String tempDirectory = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
        Transaction tx = simpleDB.newTransaction();
        MetadataManager metadataManager = simpleDB.getMetadataManager();

        Schema schema = new Schema();
        schema.addIntField("field", false);

        metadataManager.createTable("tbl1", schema, tx);

        metadataManager.createIndex("index1", "tbl1", "field", IndexType.HASH, tx);
        assertThrowsExactly(IndexDuplicateNameException.class,
                () ->  metadataManager.createIndex("index1", "tbl1", "field", IndexType.HASH, tx));

        tx.commit();
    }

    @Test
    public void testIndexCreationForUnknownTableOrField() throws IOException {
        String tempDirectory = TestUtils.setUpTempDirectory();

        SimpleDB simpleDB = new SimpleDB(tempDirectory);
        Transaction tx = simpleDB.newTransaction();
        MetadataManager metadataManager = simpleDB.getMetadataManager();

        Schema schema = new Schema();
        schema.addIntField("field", false);

        metadataManager.createTable("tbl1", schema, tx);

        assertThrowsExactly(IndexTableIncorrectException.class,
                () ->  metadataManager.createIndex("index1", "tbl2", "field", IndexType.HASH, tx));
        assertThrowsExactly(IndexTableIncorrectException.class,
                () ->  metadataManager.createIndex("index1", "tbl1", "field1", IndexType.HASH, tx));

        tx.commit();
    }
}
