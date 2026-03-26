package com.luka.lbdb.parsing.parser.parseTypes;

import com.luka.lbdb.parsing.exceptions.ParsingException;
import com.luka.lbdb.parsing.parser.Parser;
import com.luka.lbdb.parsing.parser.ParserContext;
import com.luka.lbdb.parsing.statement.UpdateStatement;
import com.luka.lbdb.parsing.statement.update.NewFieldExpressionAssignment;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParseUpdateTests {
    private UpdateStatement parse(String query) {
        ParserContext ctx = new ParserContext(query);
        return new ParseUpdate(ctx).parse();
    }

    @Test
    public void parseSingleAssignment() {
        String query = "UPDATE t SET a = 1";
        assertEquals("UPDATE t SET a = 1;", parse(query).toString());
    }

    @Test
    public void parseMultipleAssignments() {
        String query = "UPDATE students SET grade = 10, age = 21, status = 'graduated'";
        String expected = "UPDATE students SET grade = 10, age = 21, status = 'graduated';";
        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseMultiAssignmentWithExpressions() {
        // Mixing constants and arithmetic expressions
        String query = "UPDATE account SET balance = balance + 100, last_updated = 2026";
        String expected = "UPDATE account SET balance = (balance + 100), last_updated = 2026;";
        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseMultiAssignmentWithWhere() {
        String query = "UPDATE emp SET sal = sal * 2, bonus = 500 WHERE dept = 10 AND performance = 'A'";
        String expected = "UPDATE emp SET sal = (sal * 2), bonus = 500 WHERE dept = 10 AND performance = 'A';";
        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseAssignmentOrderPreservation() {
        UpdateStatement stmt = parse("UPDATE t SET x = 1, y = 2");
        List<NewFieldExpressionAssignment> assignments = stmt.newValues();

        assertEquals(2, assignments.size());
        assertEquals("x", assignments.get(0).fieldName());
        assertEquals("y", assignments.get(1).fieldName());
    }

    @Test
    public void parseFailDanglingComma() {
        assertThrows(ParsingException.class, () -> parse("UPDATE t SET a = 1, "));
    }

    @Test
    public void parseFailMissingAssignmentAfterComma() {
        assertThrows(ParsingException.class, () -> parse("UPDATE t SET a = 1, WHERE b = 2"));
    }

    @Test
    public void parseFailMissingCommaBetweenAssignments() {
        // full parser needed here
        assertThrows(ParsingException.class, () -> new Parser("UPDATE t SET a = 1 b = 2;").parse());
    }

    @Test
    public void parseFailIncompleteAssignment() {
        assertThrows(ParsingException.class, () -> parse("UPDATE t SET a = , b = 2"));
    }

    @Test
    public void parseFailMissingSetKeyword() {
        assertThrows(ParsingException.class, () -> parse("UPDATE t a = 1, b = 2"));
    }

    @Test
    public void parseFailInvalidFieldToken() {
        assertThrows(ParsingException.class, () -> parse("UPDATE t SET 123 = 1"));
    }
}
