package com.luka.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.exceptions.ParserException;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.parser.parseTypes.ParseCreateTable;
import com.luka.simpledb.parsingManagement.statement.CreateTableStatement;
import com.luka.simpledb.recordManagement.exceptions.FieldDuplicateNameException;
import com.luka.simpledb.recordManagement.exceptions.FieldLimitException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ParseCreateTableTests {
    private CreateTableStatement parse(String query) {
        ParserContext ctx = new ParserContext(query);
        return new ParseCreateTable(ctx).parse();
    }

    @Test
    public void parseAllTypesWithNullability() {
        String query = "TABLE users (id INT NOT NULL, active BOOLEAN, username VARCHAR(20) NOT NULL)";

        CreateTableStatement stmt = parse(query);
        assertEquals("users", stmt.tableName());
        assertTrue(stmt.schema().hasField("id"));
        assertFalse(stmt.schema().isNullable("id"));
    }

    @Test
    public void parseVarcharWithConstantExpression() {
        String query = "TABLE cache (key VARCHAR(10 + 20 * 2), val VARCHAR(50))";
        CreateTableStatement stmt = parse(query);

        assertEquals(50, stmt.schema().length("key"));
        assertEquals(50, stmt.schema().length("val"));
    }

    @Test
    public void parseBasicIntTable() {
        String query = "TABLE t1 (id INT)";
        String expected = "CREATE TABLE t1 (id INT);";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseBasicBooleanTable() {
        String query = "TABLE t2 (is_active BOOLEAN)";
        String expected = "CREATE TABLE t2 (is_active BOOLEAN);";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseVarcharWithLiteral() {
        String query = "TABLE t3 (name VARCHAR(50))";
        String expected = "CREATE TABLE t3 (name VARCHAR(50));";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseVarcharWithExpressionFolding() {
        String query = "TABLE t4 (description VARCHAR(10 + 5 * 2))";
        String expected = "CREATE TABLE t4 (description VARCHAR(20));";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseVarcharWithNestedExpression() {
        String query = "TABLE t5 (data VARCHAR((100 / 2) - 10))";
        String expected = "CREATE TABLE t5 (data VARCHAR(40));";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseNotNullConstraint() {
        String query = "TABLE t6 (id INT NOT NULL)";
        String expected = "CREATE TABLE t6 (id INT NOT NULL);";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseMultipleFieldsMixedTypes() {
        String query = "TABLE users (id INT NOT NULL, username VARCHAR(20), active BOOLEAN NOT NULL)";
        String expected = "CREATE TABLE users (id INT NOT NULL, username VARCHAR(20), active BOOLEAN NOT NULL);";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseDuplicateFieldFail() {
        String query = "TABLE bad_table (col1 INT, col1 BOOLEAN)";
        assertThrows(FieldDuplicateNameException.class, () -> parse(query));
    }

    @Test
    public void parseFieldLimitFail() {
        StringBuilder sb = new StringBuilder("TABLE big_table (");
        for (int i = 0; i < 32; i++) {
            sb.append("col").append(i).append(" INT, ");
        }
        String query = sb.substring(0, sb.length() - 2) + ")";

        assertThrows(FieldLimitException.class, () -> parse(query));
    }

    @Test
    public void parseVarcharWithNonIntegerFail() {
        assertThrows(ParserException.class, () -> parse("TABLE t (name VARCHAR('ten'))"));
    }

    @Test
    public void parseVarcharWithNonConstantFail() {
        assertThrows(ParserException.class, () -> parse("TABLE t (name VARCHAR(id))"));
    }

    @Test
    public void parseFailIncompleteNotNull() {
        assertThrows(ParserException.class, () -> parse("TABLE t (id INT NOT)"));
    }

    @Test
    public void parseFailInvalidType() {
        assertThrows(ParserException.class, () -> parse("TABLE t (id DOUBLE)"));
    }

    @Test
    public void parseFailMissingParens() {
        assertThrows(ParserException.class, () -> parse("TABLE t id INT, name VARCHAR(10)"));
    }

    @Test
    public void parseFailDanglingComma() {
        assertThrows(ParserException.class, () -> parse("TABLE t (id INT, )"));
    }

    @Test
    public void parseFailMissingTableName() {
        assertThrows(ParserException.class, () -> parse("TABLE (id INT)"));
    }

    @Test
    public void parseFailEmptyFields() {
        assertThrows(ParserException.class, () -> parse("TABLE t7 ()"));
    }

    @Test
    public void parseFailUnclosedParenthesis() {
        assertThrows(ParserException.class, () -> parse("TABLE t11 (id INT"));
    }
}
