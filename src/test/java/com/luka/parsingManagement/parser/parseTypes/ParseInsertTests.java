package com.luka.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.exceptions.ParsingException;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.parser.parseTypes.ParseInsert;
import com.luka.simpledb.parsingManagement.statement.InsertStatement;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ParseInsertTests {
    private InsertStatement parse(String query) {
        ParserContext ctx = new ParserContext(query);
        return new ParseInsert(ctx).parse();
    }

    @Test
    public void parseSimpleInsert() {
        String query = "INSERT INTO my_table (id, name) VALUES (1, 'Alice')";
        String expected = "INSERT INTO my_table (id, name) VALUES (1, 'Alice');";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseSingleFieldInsert() {
        String query = "INSERT INTO config (timeout) VALUES (500)";
        String expected = "INSERT INTO config (timeout) VALUES (500);";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseInsertWithConstantFoldingArithmetic() {
        String query = "INSERT INTO stats (score, multiplier) VALUES (10 + 5, 2 * (3 + 4))";
        String expected = "INSERT INTO stats (score, multiplier) VALUES (15, 14);";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseInsertWithConstantFoldingUnary() {
        String query = "INSERT INTO balances (amount) VALUES (-(-100) + 50)";
        String expected = "INSERT INTO balances (amount) VALUES (150);";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseInsertWithVariousDataTypes() {
        String query = "INSERT INTO flags (is_active, code, ratio) VALUES (TRUE, 'OK', 1)";
        String expected = "INSERT INTO flags (is_active, code, ratio) VALUES (TRUE, 'OK', 1);";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseFailNonConstantExpression() {
        String query = "INSERT INTO users (id, age) VALUES (1, age + 1)";

        ParsingException exception = assertThrows(ParsingException.class, () -> parse(query));
        assertTrue(exception.getMessage().contains("constant expressions"));
    }

    @Test
    public void parseFailMissingInsertKeyword() {
        assertThrows(ParsingException.class, () -> parse("INTO users (id) VALUES (1)"));
    }

    @Test
    public void parseFailMissingIntoKeyword() {
        assertThrows(ParsingException.class, () -> parse("INSERT users (id) VALUES (1)"));
    }

    @Test
    public void parseFailMissingValuesKeyword() {
        assertThrows(ParsingException.class, () -> parse("INSERT INTO users (id) (1)"));
    }

    @Test
    public void parseFailMissingTableIdentifier() {
        assertThrows(ParsingException.class, () -> parse("INSERT INTO (id) VALUES (1)"));
    }

    @Test
    public void parseFailMissingLeftParenForFields() {
        assertThrows(ParsingException.class, () -> parse("INSERT INTO users id, name) VALUES (1, 'Alice')"));
    }

    @Test
    public void parseFailMissingRightParenForFields() {
        assertThrows(ParsingException.class, () -> parse("INSERT INTO users (id, name VALUES (1, 'Alice')"));
    }

    @Test
    public void parseFailMissingLeftParenForValues() {
        assertThrows(ParsingException.class, () -> parse("INSERT INTO users (id) VALUES 1)"));
    }

    @Test
    public void parseFailMissingRightParenForValues() {
        assertThrows(ParsingException.class, () -> parse("INSERT INTO users (id) VALUES (1"));
    }

    @Test
    public void parseFailDanglingCommaInFields() {
        assertThrows(ParsingException.class, () -> parse("INSERT INTO users (id,) VALUES (1)"));
    }

    @Test
    public void parseFailDanglingCommaInValues() {
        assertThrows(ParsingException.class, () -> parse("INSERT INTO users (id) VALUES (1,)"));
    }
}
