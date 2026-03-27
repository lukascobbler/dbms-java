package com.luka.lbdb.metadataManagement;

import com.luka.lbdb.metadataManagement.exceptions.IndexDuplicateNameException;
import com.luka.lbdb.metadataManagement.exceptions.IndexTableIncorrectException;
import com.luka.lbdb.metadataManagement.exceptions.TableNotFoundException;
import com.luka.lbdb.metadataManagement.infoClasses.IndexType;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.db.LBDB;
import com.luka.lbdb.transactionManagement.Transaction;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class IndexMetadataManagerTests {
    @Test
    public void testDuplicateIndex() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();

        LBDB LBDB = new LBDB(tmpDir);
        Transaction tx = LBDB.getTransactionManager().getOrCreateTransaction(-1);
        MetadataManager metadataManager = LBDB.getMetadataManager();

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
        Path tmpDir = TestUtils.setUpTempDirectory();

        LBDB LBDB = new LBDB(tmpDir);
        Transaction tx = LBDB.getTransactionManager().getOrCreateTransaction(-1);
        MetadataManager metadataManager = LBDB.getMetadataManager();

        Schema schema = new Schema();
        schema.addIntField("field", false);

        metadataManager.createTable("tbl1", schema, tx);

        assertThrowsExactly(TableNotFoundException.class,
                () ->  metadataManager.createIndex("index1", "tbl2", "field", IndexType.HASH, tx));
        assertThrowsExactly(IndexTableIncorrectException.class,
                () ->  metadataManager.createIndex("index1", "tbl1", "field1", IndexType.HASH, tx));

        tx.commit();
    }
}
