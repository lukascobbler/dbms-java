package com.luka.queryManagement.scanTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.scanDefinitions.Scan;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.*;
import com.luka.simpledb.queryManagement.scanTypes.update.SelectScan;
import com.luka.simpledb.queryManagement.scanTypes.update.TableScan;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.constant.IntConstant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.*;
import com.luka.simpledb.queryManagement.virtualEntities.term.Term;
import com.luka.simpledb.queryManagement.virtualEntities.term.TermOperator;
import com.luka.simpledb.recordManagement.Layout;
import com.luka.simpledb.recordManagement.Schema;
import com.luka.simpledb.simpleDB.SimpleDB;
import com.luka.simpledb.transactionManagement.Transaction;
import com.luka.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CombiningScanTests {
    @Test
    public void testComplexAntiJoinWithTransformation() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoTables(tmpDir);

        Term filterInner = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.LESS_OR_EQUAL,
                new ConstantExpression(new IntConstant(10))
        );
        Predicate innerPred = new Predicate(filterInner);

        Term joinTerm = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new FieldNameExpression("t2_intField1")
        );
        Predicate joinPred = new Predicate(joinTerm);

        Expression bonusExpr = new BinaryArithmeticExpression(
                new FieldNameExpression("t1_intField1"),
                ArithmeticOperator.MUL,
                new ConstantExpression(new IntConstant(10))
        );

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan innerSelect = new SelectScan(s2, innerPred);
             AntijoinScan antiJoin = new AntijoinScan(s1, innerSelect, joinPred);
             ExtendProjectScan extend = new ExtendProjectScan(antiJoin, Map.of(
                     "bonus_score", bonusExpr,
                     "t1_intField1", new FieldNameExpression("t1_intField1")
             ));
             RenameScan finalScan = new RenameScan(extend, Map.of("user_id", "t1_intField1"))) {

            finalScan.beforeFirst();
            assertTrue(finalScan.next());

            assertEquals(11, finalScan.getInt("user_id"));
            assertEquals(110, finalScan.getInt("bonus_score"));
            assertFalse(finalScan.hasField("t1_intField1"));
        }
    }

    @Test
    public void testProductUnionSemiJoinPipeline() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoTables(tmpDir);

        Term pairTerm1 = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(5))
        );
        Term pairTerm2 = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(5))
        );
        Predicate pairPred = new Predicate(pairTerm1, pairTerm2);

        Term semiTerm = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.GREATER_THAN,
                new ConstantExpression(new IntConstant(248))
        );
        Predicate semiPred = new Predicate(semiTerm);

        try (UpdateScan s1a = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2a = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             ProductScan product = new ProductScan(s1a, s2a);
             SelectReadOnlyScan selectPair = new SelectReadOnlyScan(product, pairPred);

             UpdateScan s1b = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2b = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SemijoinScan semiJoin = new SemijoinScan(s1b, s2b, semiPred);

             UnionScan finalUnion = new UnionScan(selectPair, semiJoin)) {

            finalUnion.beforeFirst();

            assertTrue(finalUnion.next());
            assertEquals(5, finalUnion.getInt("t1_intField1"));

            int count = 1;
            while (finalUnion.next()) {
                count++;
            }
            assertEquals(251, count);
        }
    }

    @Test
    public void testFilteringByExtendedFieldInAntiJoin() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoTables(tmpDir);

        Expression constExpr = new ConstantExpression(new IntConstant(100));

        Term joinTerm = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new FieldNameExpression("match_key")
        );
        Predicate joinPred = new Predicate(joinTerm);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             ExtendProjectScan extendedS2 = new ExtendProjectScan(s2, Map.of("match_key", constExpr));
             AntijoinScan antiJoin = new AntijoinScan(s1, extendedS2, joinPred)) {

            antiJoin.beforeFirst();
            int count = 0;
            while (antiJoin.next()) {
                assertNotEquals(100, antiJoin.getInt("t1_intField1"));
                count++;
            }
            assertEquals(249, count);
        }
    }

    // UPDATE table1
    // SET t1_intField1 = t1_intField1 + 1000, t1_stringField1 = 'UPDATED'
    // WHERE NOT EXISTS (
    //     SELECT 1
    //     FROM table2
    //     WHERE table1.t1_intField1 = table2.t2_intField1 AND table2.t2_intField1 < 5
    // );
    @Test
    public void testDeeplyNestedUpdatePipeline() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoTables(tmpDir);

        Term protTerm = new Term(
                new FieldNameExpression("t2_intField1"),
                TermOperator.LESS_THAN,
                new ConstantExpression(new IntConstant(5))
        );
        Predicate protPred = new Predicate(protTerm);

        Term joinTerm = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new FieldNameExpression("t2_intField1")
        );
        Predicate joinPred = new Predicate(joinTerm);

        Expression plusTen = new BinaryArithmeticExpression(
                new FieldNameExpression("t1_intField1"),
                ArithmeticOperator.ADD,
                new ConstantExpression(new IntConstant(1000))
        );

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SelectScan protectedS2 = new SelectScan(s2, protPred);
             AntijoinScan targetRows = new AntijoinScan(s1, protectedS2, joinPred);
             ExtendProjectScan extended = new ExtendProjectScan(targetRows, Map.of(
                     "temp_val", plusTen,
                     "t1_intField1", new FieldNameExpression("t1_intField1")
                     ))) {

            extended.beforeFirst();
            int updatedCount = 0;

            while (extended.next()) {
                int originalId = extended.getInt("t1_intField1");
                int newValue = extended.getInt("temp_val");

                s1.beforeFirst();
                while (s1.next()) {
                    if (s1.getInt("t1_intField1") == originalId) {
                        s1.setInt("t1_intField1", newValue); // The actual Update
                        s1.setString("t1_stringField1", "UPDATED");
                        updatedCount++;
                        break;
                    }
                }
            }

            assertEquals(245, updatedCount);

            s1.beforeFirst();
            int i = 0;
            while (s1.next()) {
                int val = s1.getInt("t1_intField1");
                if (val < 1000) {
                    assertTrue(val >= 0 && val < 5);
                    assertNotEquals("UPDATED", s1.getString("t1_stringField1"));
                } else {
                    assertTrue(val >= 1005);
                    assertEquals("UPDATED", s1.getString("t1_stringField1"));
                }
                i++;
            }
        }
    }

    @Test
    public void testUnionUpdateTransparency() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoTables(tmpDir);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             RenameScan rs2 = new RenameScan(s2, Map.of("t1_intField1", "t2_intField1"));
             UnionScan union = new UnionScan(s1, rs2)) {

            s2.beforeFirst();
            s2.next();
            s2.setInt("t2_intField1", 9999);

            union.beforeFirst();
            boolean found = false;
            while (union.next()) {
                if (union.getInt("t1_intField1") == 9999) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    @Test
    public void profileHasFieldValidationChecks() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        SimpleDB simpleDB = new SimpleDB(tmpDir);
        Transaction tx = simpleDB.newTransaction();

        Schema sch = new Schema();
        String baseFieldName = "t1_intField1";
        sch.addIntField(baseFieldName, false);
        simpleDB.getMetadataManager().createTable("table1", sch, tx);
        Layout layout = simpleDB.getMetadataManager().getLayout("table1", tx);

        try (UpdateScan tableScan = new TableScan(tx, "table1", layout)) {
            tableScan.beforeFirst();
            tableScan.insert();
            tableScan.setInt(baseFieldName, 50);
        }

        int depth = 1000;
        Scan currentScan = new TableScan(tx, "table1", layout);

        String lastName = baseFieldName;
        for (int i = 1; i <= depth; i++) {
            String nextName = "r" + i;
            currentScan = new RenameScan(currentScan, Map.of(nextName, lastName));
            lastName = nextName;
        }

        try (Scan finalScan = currentScan) {
            finalScan.beforeFirst();
            assertTrue(finalScan.next());

            String topLevelName = "r" + depth;
            String penultimateName = "r" + (depth - 1);

            // Verification
            assertTrue(finalScan.hasField(topLevelName));
            assertFalse(finalScan.hasField(penultimateName));
            assertFalse(finalScan.hasField(baseFieldName));

            assertEquals(50, finalScan.getInt(topLevelName));

            assertThrowsExactly(FieldNotFoundInScanException.class, () -> finalScan.getInt(penultimateName));
            assertThrowsExactly(FieldNotFoundInScanException.class, () -> finalScan.getInt(baseFieldName));
        }
    }
}
