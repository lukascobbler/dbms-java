package com.luka.lbdb.querying.scanTests;

import com.luka.lbdb.querying.QueryTestUtils;
import com.luka.lbdb.querying.exceptions.FieldNotFoundInScanException;
import com.luka.lbdb.querying.scanDefinitions.Scan;
import com.luka.lbdb.querying.scanDefinitions.UpdateScan;
import com.luka.lbdb.querying.scanTypes.readOnly.*;
import com.luka.lbdb.querying.scanTypes.update.SelectScan;
import com.luka.lbdb.querying.scanTypes.update.TableScan;
import com.luka.lbdb.querying.virtualEntities.Predicate;
import com.luka.lbdb.querying.virtualEntities.constant.IntConstant;
import com.luka.lbdb.querying.virtualEntities.constant.StringConstant;
import com.luka.lbdb.querying.virtualEntities.expression.*;
import com.luka.lbdb.querying.virtualEntities.term.Term;
import com.luka.lbdb.querying.virtualEntities.term.TermOperator;
import com.luka.lbdb.records.Layout;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.db.LBDB;
import com.luka.lbdb.transactionManagement.Transaction;
import com.luka.lbdb.testUtils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CombiningScanTests {
    @Test
    public void testComplexAntiJoinWithTransformation() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

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

            assertEquals(11, finalScan.getValue("user_id").asInt());
            assertEquals(110, finalScan.getValue("bonus_score").asInt());
            assertFalse(finalScan.hasField("t1_intField1"));
        }
    }

    @Test
    public void testProductUnionSemiJoinPipeline() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

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

             UnionAllScan finalUnion = new UnionAllScan(selectPair, semiJoin)) {

            finalUnion.beforeFirst();

            assertTrue(finalUnion.next());
            assertEquals(5, finalUnion.getValue("t1_intField1").asInt());

            int count = 1;
            while (finalUnion.next()) {
                count++;
            }
            assertEquals(251, count);
        }
    }

    @Test
    public void testFilteringByExtendedFieldInAntiJoin() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

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
                assertNotEquals(100, antiJoin.getValue("t1_intField1").asInt());
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
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

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
                int originalId = extended.getValue("t1_intField1").asInt();
                int newValue = extended.getValue("temp_val").asInt();

                s1.beforeFirst();
                while (s1.next()) {
                    if (s1.getValue("t1_intField1").asInt() == originalId) {
                        s1.setValue("t1_intField1", new IntConstant(newValue)); // The actual Update
                        s1.setValue("t1_stringField1", new StringConstant("UPDATED"));
                        updatedCount++;
                        break;
                    }
                }
            }

            assertEquals(245, updatedCount);

            s1.beforeFirst();
            int i = 0;
            while (s1.next()) {
                int val = s1.getValue("t1_intField1").asInt();
                if (val < 1000) {
                    assertTrue(val >= 0 && val < 5);
                    assertNotEquals("UPDATED", s1.getValue("t1_stringField1").asString());
                } else {
                    assertTrue(val >= 1005);
                    assertEquals("UPDATED", s1.getValue("t1_stringField1").asString());
                }
                i++;
            }
        }
    }

    @Test
    public void testUnionAllUpdateTransparency() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeTwoFullTables(tmpDir);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             RenameScan rs2 = new RenameScan(s2, Map.of("t1_intField1", "t2_intField1"));
             UnionAllScan union = new UnionAllScan(s1, rs2)) {

            s2.beforeFirst();
            s2.next();
            s2.setValue("t2_intField1", new IntConstant(9999));

            union.beforeFirst();
            boolean found = false;
            while (union.next()) {
                if (union.getValue("t1_intField1").asInt() == 9999) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    @Test
    public void profileHasFieldValidationChecks() throws IOException {
        Path tmpDir = TestUtils.setUpTempDirectory();
        LBDB LBDB = new LBDB(tmpDir);
        Transaction tx = LBDB.getTransactionManager().getOrCreateTransaction(-1);

        Schema sch = new Schema();
        String baseFieldName = "t1_intField1";
        sch.addIntField(baseFieldName, false);
        LBDB.getMetadataManager().createTable("table1", sch, tx);
        Layout layout = LBDB.getMetadataManager().getLayout("table1", tx);

        try (UpdateScan tableScan = new TableScan(tx, "table1", layout)) {
            tableScan.beforeFirst();
            tableScan.insert();
            tableScan.setValue(baseFieldName, new IntConstant(50));
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

            assertEquals(50, finalScan.getValue(topLevelName).asInt());

            assertThrowsExactly(FieldNotFoundInScanException.class, () -> finalScan.getValue(penultimateName).asInt());
            assertThrowsExactly(FieldNotFoundInScanException.class, () -> finalScan.getValue(baseFieldName).asInt());
        }
    }
}
