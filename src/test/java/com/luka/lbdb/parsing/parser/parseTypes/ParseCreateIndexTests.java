package com.luka.lbdb.parsing.parser.parseTypes;

import com.luka.lbdb.parsing.exceptions.ParsingException;
import com.luka.lbdb.parsing.parser.ParserContext;
import com.luka.lbdb.parsing.statement.CreateIndexStatement;
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
        String query = "INDEX idx_emp_name ON employees (last_name) TYPE HASH";
        String expected = "CREATE INDEX idx_emp_name ON employees (last_name) TYPE HASH;";

        assertEquals(expected, parse(query).toString());
    }

    @Test
    public void parseFailMissingIndexKeyword() {
        assertThrows(ParsingException.class, () -> parse("idx_emp_name ON employees (last_name)"));
    }

    @Test
    public void parseFailMissingTypeKeyword() {
        assertThrows(ParsingException.class, () -> parse("INDEX idx_emp_name ON employees (last_name) TYPE"));
    }

    @Test
    public void parseFailInvalidType() {
        assertThrows(ParsingException.class, () -> parse("INDEX idx_emp_name ON employees (last_name) TYPE X"));
    }

    @Test
    public void parseFailMultipleFields1() {
        assertThrows(ParsingException.class, () -> parse("idx_emp_name ON employees (last_name, a)"));
    }

    public void parseFailMultipleFields2() {
        assertThrows(ParsingException.class, () -> parse("idx_emp_name ON employees (last_name a)"));
    }

    @Test
    public void parseFailMissingIndexName() {
        assertThrows(ParsingException.class, () -> parse("INDEX ON employees (last_name)"));
    }

    @Test
    public void parseFailMissingOnKeyword() {
        assertThrows(ParsingException.class, () -> parse("INDEX idx_emp_name employees (last_name)"));
    }

    @Test
    public void parseFailMissingTableName() {
        assertThrows(ParsingException.class, () -> parse("INDEX idx_emp_name ON (last_name)"));
    }

    @Test
    public void parseFailMissingLeftParenthesis() {
        assertThrows(ParsingException.class, () -> parse("INDEX idx_emp_name ON employees last_name)"));
    }

    @Test
    public void parseFailMissingFieldName() {
        assertThrows(ParsingException.class, () -> parse("INDEX idx_emp_name ON employees ()"));
    }

    @Test
    public void parseFailMissingRightParenthesis() {
        assertThrows(ParsingException.class, () -> parse("INDEX idx_emp_name ON employees (last_name"));
    }
}
