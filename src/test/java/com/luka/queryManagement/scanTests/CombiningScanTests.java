package com.luka.queryManagement.scanTests;

import com.luka.queryManagement.QueryTestUtils;
import com.luka.simpledb.queryManagement.exceptions.FieldNotFoundInScanException;
import com.luka.simpledb.queryManagement.scanDefinitions.UpdateScan;
import com.luka.simpledb.queryManagement.scanTypes.readOnly.*;
import com.luka.simpledb.queryManagement.scanTypes.update.ProjectScan;
import com.luka.simpledb.queryManagement.scanTypes.update.RenameScan;
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
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CombiningScanTests {
    @Test
    public void testComplexAntiJoinWithTransformation() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndTwoTables(tmpDir);

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
             ExtendScan extend = new ExtendScan(antiJoin, bonusExpr, "bonus_score");
             RenameReadOnlyScan rename = new RenameReadOnlyScan(extend, "t1_intField1", "user_id");
             ProjectReadOnlyScan finalScan = new ProjectReadOnlyScan(rename, Arrays.asList("user_id", "bonus_score"))) {

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
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndTwoTables(tmpDir);

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
             ProjectReadOnlyScan projectA = new ProjectReadOnlyScan(selectPair, List.of("t1_intField1"));

             UpdateScan s1b = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2b = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             SemijoinScan semiJoin = new SemijoinScan(s1b, s2b, semiPred);
             ProjectReadOnlyScan projectB = new ProjectReadOnlyScan(semiJoin, List.of("t1_intField1"));

             UnionScan finalUnion = new UnionScan(projectA, projectB)) {

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
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndTwoTables(tmpDir);

        Expression constExpr = new ConstantExpression(new IntConstant(100));

        Term joinTerm = new Term(
                new FieldNameExpression("t1_intField1"),
                TermOperator.EQUALS,
                new FieldNameExpression("match_key")
        );
        Predicate joinPred = new Predicate(joinTerm);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             ExtendScan extendedS2 = new ExtendScan(s2, constExpr, "match_key");
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
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndTwoTables(tmpDir);

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
             ExtendScan extended = new ExtendScan(targetRows, plusTen, "temp_val");
             RenameScan renamed = new RenameScan(s1, "t1_intField1", "updatable_field");
             ProjectScan updatable = new ProjectScan(renamed, Arrays.asList("updatable_field", "t1_intField2"))) {

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
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndTwoTables(tmpDir);

        try (UpdateScan s1 = new TableScan(testData.tx(), "table1", testData.layouts().get(0));
             UpdateScan s2 = new TableScan(testData.tx(), "table2", testData.layouts().get(1));
             RenameScan rs2 = new RenameScan(s2, "t2_intField1", "t1_intField1");
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
    public void testUpdatePersistenceThroughDeeplyNestedLayers() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        Term t1 = new Term(
                new FieldNameExpression("level3_int"),
                TermOperator.EQUALS,
                new ConstantExpression(new IntConstant(10))
        );

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst())) {
            UpdateScan layer1 = new RenameScan(tableScan, "t1_intField1", "level1_int");
            UpdateScan layer2 = new ProjectScan(layer1, Arrays.asList("level1_int", "t1_stringField1"));
            UpdateScan layer3 = new RenameScan(layer2, "level1_int", "level3_int");
            UpdateScan layer4 = new SelectScan(layer3, new Predicate(t1));
            UpdateScan layer5 = new RenameScan(layer4, "level3_int", "final_id");

            layer5.beforeFirst();
            assertTrue(layer5.next());

            layer5.setInt("final_id", 999);
            layer5.setString("t1_stringField1", "LayerUpdate");

            assertEquals(999, layer5.getInt("final_id"));

            tableScan.beforeFirst();
            boolean found = false;
            while (tableScan.next()) {
                if (tableScan.getInt("t1_intField1") == 999) {
                    assertEquals("LayerUpdate", tableScan.getString("t1_stringField1"));
                    found = true;
                }
            }
            assertTrue(found);
        }
    }

    @Test
    public void testUpdatePersistenceAndRestrictionThroughCombinedLayers() throws IOException {
        String tmpDir = TestUtils.setUpTempDirectory();
        QueryTestUtils.QueryTestData testData = QueryTestUtils.initializeSystemAndOneTable(tmpDir);

        try (UpdateScan tableScan = new TableScan(testData.tx(), "table1", testData.layouts().getFirst())) {
            UpdateScan renameLayer = new RenameScan(tableScan, "t1_intField1", "id");
            UpdateScan renameLayer2 = new RenameScan(renameLayer, "t1_stringField1", "name");

            UpdateScan projectLayer = new ProjectScan(renameLayer2, Arrays.asList("id", "name"));

            Term t = new Term(
                    new FieldNameExpression("id"),
                    TermOperator.EQUALS,
                    new ConstantExpression(new IntConstant(20))
            );
            UpdateScan finalView = new SelectScan(projectLayer, new Predicate(t));

            finalView.beforeFirst();
            assertTrue(finalView.next());

            finalView.setString("name", "New Identity");
            finalView.setInt("id", 888);

            assertThrows(FieldNotFoundInScanException.class, () ->
                    finalView.setInt("t1_intField2", 555));

            tableScan.beforeFirst();
            boolean found = false;
            while (tableScan.next()) {
                if (tableScan.getInt("t1_intField1") == 888) {
                    assertEquals("New Identity", tableScan.getString("t1_stringField1"));
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
        UpdateScan currentScan = new TableScan(tx, "table1", layout);

        String lastName = baseFieldName;
        for (int i = 1; i <= depth; i++) {
            String nextName = "r" + i;
            currentScan = new RenameScan(currentScan, lastName, nextName);
            lastName = nextName;
        }

        try (UpdateScan finalScan = currentScan) {
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
