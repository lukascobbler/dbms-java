package com.luka.parsingManagement.parser;

import com.luka.simpledb.parsingManagement.exceptions.ParsingException;
import com.luka.simpledb.parsingManagement.parser.Parser;
import com.luka.simpledb.parsingManagement.statement.CreateIndexStatement;
import com.luka.simpledb.parsingManagement.statement.ExplainStatement;
import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.parsingManagement.statement.Statement;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ParserTests {
    @Test
    public void testParseSelectSimple() {
        String query = "SELECT id, name FROM users;";
        Parser parser = new Parser(query);
        Statement stmt = parser.parse();

        assertEquals("SELECT id, name FROM users;", stmt.toString());
    }

    @Test
    public void testParseSelectWithWhereAndJoin() {
        String query =
                "SELECT userId, profileBio " +
                        "FROM users JOIN profiles ON userId = profileId " +
                        "WHERE userAge >= 18;";
        Parser parser = new Parser(query);
        Statement stmt = parser.parse();

        // join predicates are always transformed and added to the where
        // predicate after the original predicate
        assertEquals("SELECT userid, profilebio " +
                "FROM users, profiles " +
                "WHERE userage >= 18 AND userid = profileid;", stmt.toString());
    }

    @Test
    public void testParseInsertConstantEvaluation() {
        String query = "INSERT INTO users (id, name) VALUES (1 + 2, 'Alice');";
        Parser parser = new Parser(query);
        Statement stmt = parser.parse();

        assertEquals("INSERT INTO users (id, name) VALUES (3, 'Alice');", stmt.toString());
    }

    @Test
    public void testParseUpdateWithExpression() {
        String query = "UPDATE inventory SET stock = stock - 1 WHERE item_id = 5;";
        Parser parser = new Parser(query);
        Statement stmt = parser.parse();

        assertEquals("UPDATE inventory SET stock = (stock - 1) WHERE item_id = 5;", stmt.toString());
    }

    @Test
    public void testParseDelete() {
        String query = "DELETE FROM sessions WHERE expired = TRUE;";
        Parser parser = new Parser(query);
        Statement stmt = parser.parse();

        assertEquals("DELETE FROM sessions WHERE expired = TRUE;", stmt.toString());
    }

    @Test
    public void testParseCreateTable() {
        String query = "CREATE TABLE employees (id INT NOT NULL, name VARCHAR(100), active BOOLEAN);";
        Parser parser = new Parser(query);
        Statement stmt = parser.parse();

        assertEquals(
                "CREATE TABLE employees (id INT NOT NULL, name VARCHAR(100), active BOOLEAN);",
                stmt.toString()
        );
    }

    @Test
    public void testParseCreateIndex() {
        String query = "CREATE INDEX idx_user_name ON users (name);";
        Parser parser = new Parser(query);
        Statement stmt = parser.parse();

        assertEquals("CREATE INDEX idx_user_name ON users (name) TYPE BTREE;", stmt.toString());
    }

    @Test
    public void testExplain1() {
        String query = "EXPLAIN CREATE INDEX idx_user_name ON users (name) TYPE BTREE;";
        Parser parser = new Parser(query);
        Statement stmt = parser.parse();
        ExplainStatement explainStatement = assertInstanceOf(ExplainStatement.class, stmt);
        assertInstanceOf(CreateIndexStatement.class, explainStatement.explainingStatement());

        assertEquals(query, stmt.toString());
    }

    @Test
    public void testExplain2() {
        String query = "EXPLAIN SELECT userid, profilebio FROM users, " +
                "profiles WHERE userage >= 18 AND userid = profileid;";
        Parser parser = new Parser(query);
        Statement stmt = parser.parse();
        ExplainStatement explainStatement = assertInstanceOf(ExplainStatement.class, stmt);
        assertInstanceOf(SelectStatement.class, explainStatement.explainingStatement());

        assertEquals(query, stmt.toString());
    }

    @Test
    public void testExpressionPrecedence() {
        String query = "SELECT result FROM math WHERE val = 1 + 2 * 3;";
        Parser parser = new Parser(query);
        Statement stmt = parser.parse();

        assertEquals("SELECT result FROM math WHERE val = (1 + (2 * 3));", stmt.toString());
    }

    @Test
    public void testMissingSemicolonThrowsException() {
        String query = "SELECT id FROM users";
        Parser parser = new Parser(query);

        Exception exception = assertThrows(ParsingException.class, parser::parse);
        assertTrue(exception.getMessage().toLowerCase().contains("expected"));
    }

    @Test
    public void testUnexpectedEndOfInput() {
        String query = "UPDATE users SET ";
        Parser parser = new Parser(query);

        assertThrows(ParsingException.class, parser::parse);
    }

    @Test
    public void testInvalidVarcharLengthExpressionThrowsException() {
        String query = "CREATE TABLE invalid_table (name VARCHAR(dynamic_field));";
        Parser parser = new Parser(query);

        ParsingException exception = assertThrows(ParsingException.class, parser::parse);
        assertTrue(exception.getMessage().contains("constant expression"));
    }

    @Test
    public void testNonConstantInsertValueThrowsException() {
        String query = "INSERT INTO users (id) VALUES (some_column);";
        Parser parser = new Parser(query);

        ParsingException exception = assertThrows(ParsingException.class, parser::parse);
        assertTrue(exception.getMessage().contains("constant expression"));
    }

    @Test
    public void testInvalidCreateTargetThrowsException() {
        String query = "CREATE DATABASE mydb;";
        Parser parser = new Parser(query);

        ParsingException exception = assertThrows(ParsingException.class, parser::parse);
        assertTrue(exception.getMessage().contains("Invalid CREATE target"));
    }
}
