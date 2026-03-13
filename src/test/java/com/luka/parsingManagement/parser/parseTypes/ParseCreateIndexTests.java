package com.luka.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.exceptions.ParserException;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.parser.parseTypes.ParseCreateIndex;
import com.luka.simpledb.parsingManagement.statement.CreateIndexStatement;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParseCreateIndexTests {
    private CreateIndexStatement parse(String query) {
        ParserContext ctx = new ParserContext(query);
        return new ParseCreateIndex(ctx).parse();
    }

    @Test
    public void parseSimpleCreateIndex() {
        String query = "INDEX idx_emp_name ON employees (last_name)";
        String expected = "CREATE INDEX idx_emp_name ON employees (last_name);";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseFailMissingIndexKeyword() {
        assertThrows(ParserException.class, () -> parse("idx_emp_name ON employees (last_name)"));
    }

    @Test
    public void parseFailMultipleFields1() {
        assertThrows(ParserException.class, () -> parse("idx_emp_name ON employees (last_name, a)"));
    }

    public void parseFailMultipleFields2() {
        assertThrows(ParserException.class, () -> parse("idx_emp_name ON employees (last_name a)"));
    }

    @Test
    public void parseFailMissingIndexName() {
        assertThrows(ParserException.class, () -> parse("INDEX ON employees (last_name)"));
    }

    @Test
    public void parseFailMissingOnKeyword() {
        assertThrows(ParserException.class, () -> parse("INDEX idx_emp_name employees (last_name)"));
    }

    @Test
    public void parseFailMissingTableName() {
        assertThrows(ParserException.class, () -> parse("INDEX idx_emp_name ON (last_name)"));
    }

    @Test
    public void parseFailMissingLeftParenthesis() {
        assertThrows(ParserException.class, () -> parse("INDEX idx_emp_name ON employees last_name)"));
    }

    @Test
    public void parseFailMissingFieldName() {
        assertThrows(ParserException.class, () -> parse("INDEX idx_emp_name ON employees ()"));
    }

    @Test
    public void parseFailMissingRightParenthesis() {
        assertThrows(ParserException.class, () -> parse("INDEX idx_emp_name ON employees (last_name"));
    }
}
