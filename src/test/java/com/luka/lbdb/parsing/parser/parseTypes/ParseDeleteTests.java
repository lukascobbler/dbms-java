package com.luka.lbdb.parsing.parser.parseTypes;

import com.luka.lbdb.parsing.exceptions.ParsingException;
import com.luka.lbdb.parsing.parser.ParserContext;
import com.luka.lbdb.parsing.statement.DeleteStatement;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParseDeleteTests {
    private DeleteStatement parse(String query) {
        ParserContext ctx = new ParserContext(query);
        return new ParseDelete(ctx).parse();
    }

    @Test
    public void parseDeleteAllRecords() {
        String query = "DELETE FROM employees";
        String expected = "DELETE FROM employees;";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseDeleteWithSimpleWhere() {
        String query = "DELETE FROM employees WHERE id = 100";
        String expected = "DELETE FROM employees WHERE id = 100;";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseDeleteWithComplexWhere() {
        String query = "DELETE FROM employees WHERE department_id = 5 AND salary < 50000";
        String expected = "DELETE FROM employees WHERE department_id = 5 AND salary < 50000;";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseFailMissingDeleteKeyword() {
        assertThrows(ParsingException.class, () -> parse("FROM employees WHERE id = 1"));
    }

    @Test
    public void parseFailMissingFromKeyword() {
        assertThrows(ParsingException.class, () -> parse("DELETE employees WHERE id = 1"));
    }

    @Test
    public void parseFailMissingTableName() {
        assertThrows(ParsingException.class, () -> parse("DELETE FROM WHERE id = 1"));
    }

    @Test
    public void parseFailEmptyWhereClause() {
        assertThrows(ParsingException.class, () -> parse("DELETE FROM employees WHERE"));
    }
}
