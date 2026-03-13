package com.luka.parsingManagement.parser;

import com.luka.simpledb.parsingManagement.exceptions.ParsingException;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.token.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ParserContextTests {
    @Test
    public void testInitializationLoadsFirstToken() {
        ParserContext ctx = new ParserContext("SELECT");
        assertEquals(KeywordToken.SELECT , ctx.current());
    }

    @Test
    public void testAdvanceMovesToNextToken() {
        ParserContext ctx = new ParserContext("SELECT id");

        assertEquals(KeywordToken.SELECT, ctx.current());
        ctx.advance();
        assertEquals(new IdentifierToken("id"), ctx.current());
    }

    @Test
    public void testEofHandling() {
        ParserContext ctx = new ParserContext("id");

        assertEquals(new IdentifierToken("id"), ctx.current());

        ctx.advance();
        assertInstanceOf(EofToken.class, ctx.current());

        ctx.advance();
        assertInstanceOf(EofToken.class, ctx.current());
    }

    @Test
    public void testEatKeywordSuccess() {
        ParserContext ctx = new ParserContext("FROM table_name");

        ctx.eat(KeywordToken.FROM);
        assertEquals(new IdentifierToken("table_name"), ctx.current());
    }

    @Test
    public void testEatKeywordFailure() {
        ParserContext ctx = new ParserContext("SELECT id");

        ParsingException ex = assertThrows(ParsingException.class, () -> ctx.eat(KeywordToken.FROM));
        assertTrue(ex.getMessage().contains("Expected"));
    }

    @Test
    public void testEatSymbolSuccess() {
        ParserContext ctx = new ParserContext("= 5");

        ctx.eat(SymbolToken.EQUAL);
        assertEquals(new IntegerToken(5), ctx.current());
    }

    @Test
    public void testEatSymbolFailure() {
        ParserContext ctx = new ParserContext("+ 5");

        ParsingException ex = assertThrows(ParsingException.class, () -> ctx.eat(SymbolToken.EQUAL));
        assertTrue(ex.getMessage().contains("Expected"));
    }

    @Test
    public void testEatIfMatchesKeywordSuccess() {
        ParserContext ctx = new ParserContext("WHERE id = 1");

        boolean matched = ctx.eatIfMatches(KeywordToken.WHERE);

        assertTrue(matched);
        assertEquals(new IdentifierToken("id"), ctx.current());
    }

    @Test
    public void testEatIfMatchesKeywordFailure() {
        ParserContext ctx = new ParserContext("id = 1");

        boolean matched = ctx.eatIfMatches(KeywordToken.WHERE);

        assertFalse(matched);
        assertEquals(new IdentifierToken("id"), ctx.current());
    }

    @Test
    public void testEatIfMatchesSymbolSuccess() {
        ParserContext ctx = new ParserContext(";");
        assertTrue(ctx.eatIfMatches(SymbolToken.SEMICOLON));
        assertInstanceOf(EofToken.class, ctx.current());
    }

    @Test
    public void testEatIfMatchesSymbolFailure() {
        ParserContext ctx = new ParserContext(",");
        assertFalse(ctx.eatIfMatches(SymbolToken.SEMICOLON));
        assertEquals(SymbolToken.COMMA, ctx.current());
    }

    @Test
    public void testEatIdentifierSuccess() {
        ParserContext ctx = new ParserContext("my_table ,");

        String identifierName = ctx.eatIdentifier();

        assertEquals("my_table", identifierName);
        assertEquals(SymbolToken.COMMA, ctx.current());
    }

    @Test
    public void testEatIdentifierFailure() {
        ParserContext ctx = new ParserContext("SELECT");

        assertThrows(ParsingException.class, ctx::eatIdentifier);
    }

    @Test
    public void testMiniParseSequence() {
        ParserContext ctx = new ParserContext("INSERT INTO t1 ( col1 )");

        ctx.eat(KeywordToken.INSERT);
        ctx.eat(KeywordToken.INTO);

        String tableName = ctx.eatIdentifier();
        assertEquals("t1", tableName);

        ctx.eat(SymbolToken.LEFT_PAREN);

        String colName = ctx.eatIdentifier();
        assertEquals("col1", colName);

        ctx.eat(SymbolToken.RIGHT_PAREN);

        assertInstanceOf(EofToken.class, ctx.current());
    }
}
