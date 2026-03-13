package com.luka.parsingManagement.tokenizer;

import com.luka.simpledb.parsingManagement.exceptions.TokenizationException;
import com.luka.simpledb.parsingManagement.tokenizer.Tokenizer;
import com.luka.simpledb.parsingManagement.tokenizer.token.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

public class TokenizerTests {
    private List<Token> tokenizeAll(String query) {
        Tokenizer tokenizer = new Tokenizer(query);
        List<Token> tokens = new ArrayList<>();

        while (tokenizer.hasNext()) {
            Token t = tokenizer.next();
            tokens.add(t);
            if (t instanceof EofToken) {
                break;
            }
        }
        return tokens;
    }

    @Test
    public void testEmptyString() {
        List<Token> tokens = tokenizeAll("");
        assertEquals(1, tokens.size());
        assertInstanceOf(EofToken.class, tokens.getFirst());
    }

    @Test
    public void testOnlyWhitespace() {
        List<Token> tokens = tokenizeAll("   \t\n  \r ");
        assertEquals(1, tokens.size());
        assertInstanceOf(EofToken.class, tokens.getFirst());
    }

    @Test
    public void testSingleCharacterSymbols() {
        String query = "= + - / , ( ) ; *";
        List<Token> tokens = tokenizeAll(query);

        assertEquals(10, tokens.size());
        assertEquals(SymbolToken.EQUAL, tokens.getFirst());
        assertEquals(SymbolToken.PLUS, tokens.get(1));
        assertEquals(SymbolToken.MINUS, tokens.get(2));
        assertEquals(SymbolToken.DIVIDE, tokens.get(3));
        assertEquals(SymbolToken.COMMA, tokens.get(4));
        assertEquals(SymbolToken.LEFT_PAREN, tokens.get(5));
        assertEquals(SymbolToken.RIGHT_PAREN, tokens.get(6));
        assertEquals(SymbolToken.SEMICOLON, tokens.get(7));
        assertEquals(SymbolToken.STAR, tokens.get(8));
        assertInstanceOf(EofToken.class, tokens.get(9));
    }

    @Test
    public void testMultiCharacterSymbols() {
        String query = "!= >= <= > <";
        List<Token> tokens = tokenizeAll(query);

        assertEquals(6, tokens.size());
        assertEquals(SymbolToken.NOT_EQUAL, tokens.getFirst());
        assertEquals(SymbolToken.GREATER_THAN_OR_EQUAL, tokens.get(1));
        assertEquals(SymbolToken.LESS_THAN_OR_EQUAL, tokens.get(2));
        assertEquals(SymbolToken.GREATER_THAN, tokens.get(3));
        assertEquals(SymbolToken.LESS_THAN, tokens.get(4));
        assertInstanceOf(EofToken.class, tokens.get(5));
    }

    @Test
    public void testIntegers() {
        String query = "123 0 99999";
        List<Token> tokens = tokenizeAll(query);

        assertEquals(4, tokens.size());
        assertEquals(new IntegerToken(123), tokens.getFirst());
        assertEquals(new IntegerToken(0), tokens.get(1));
        assertEquals(new IntegerToken(99999), tokens.get(2));
    }

    @Test
    public void testStringsSingleAndDoubleQuotes() {
        String query = "'hello' \"world\" '' \"\"";
        List<Token> tokens = tokenizeAll(query);

        assertEquals(5, tokens.size());
        assertEquals(new StringToken("hello"), tokens.getFirst());
        assertEquals(new StringToken("world"), tokens.get(1));
        assertEquals(new StringToken(""), tokens.get(2));
        assertEquals(new StringToken(""), tokens.get(3));
    }

    @Test
    public void testUnclosedStringThrowsException() {
        Tokenizer tokenizer = new Tokenizer("'unclosed string literal");
        assertThrows(TokenizationException.class, tokenizer::next);
    }

    @Test
    public void testIdentifiersAreLowercased() {
        String query = "My_TaBlE_123 anotherField";
        List<Token> tokens = tokenizeAll(query);

        assertEquals(3, tokens.size());
        assertEquals(new IdentifierToken("my_table_123"), tokens.getFirst());
        assertEquals(new IdentifierToken("anotherfield"), tokens.get(1));
    }

    @Test
    public void testKeywordsParsedCorrectly() {
        String query = "SELECT FROM WHERE";
        List<Token> tokens = tokenizeAll(query);

        assertEquals(4, tokens.size());
        assertEquals(KeywordToken.SELECT, tokens.getFirst());
        assertEquals(KeywordToken.FROM, tokens.get(1));
        assertEquals(KeywordToken.WHERE, tokens.get(2));
    }

    @Test
    public void testInvalidLoneBang() {
        String query = "!";
        List<Token> tokens = tokenizeAll(query);

        assertEquals(2, tokens.size());
        assertEquals(new InvalidToken("!"), tokens.getFirst());
    }

    @Test
    public void testUnrecognizedCharacters() {
        String query = "@ # ~";
        List<Token> tokens = tokenizeAll(query);

        assertEquals(4, tokens.size());
        assertEquals(new InvalidToken("@"), tokens.getFirst());
        assertEquals(new InvalidToken("#"), tokens.get(1));
        assertEquals(new InvalidToken("~"), tokens.get(2));
    }

    @Test
    public void testIteratorExceptionAfterEof() {
        Tokenizer tokenizer = new Tokenizer("1");
        assertTrue(tokenizer.hasNext());
        assertInstanceOf(IntegerToken.class, tokenizer.next());
        assertTrue(tokenizer.hasNext());
        assertInstanceOf(EofToken.class, tokenizer.next());
        assertFalse(tokenizer.hasNext());
        assertThrows(NoSuchElementException.class, tokenizer::next);
    }

    @Test
    public void testRealisticQuery() {
        String query = "SELECT id, name FROM users WHERE age >= 18 AND status != 'banned';";
        List<Token> tokens = tokenizeAll(query);

        Token[] expected = {
                KeywordToken.SELECT,
                new IdentifierToken("id"),
                SymbolToken.COMMA,
                new IdentifierToken("name"),
                KeywordToken.FROM,
                new IdentifierToken("users"),
                KeywordToken.WHERE,
                new IdentifierToken("age"),
                SymbolToken.GREATER_THAN_OR_EQUAL,
                new IntegerToken(18),
                KeywordToken.AND,
                new IdentifierToken("status"),
                SymbolToken.NOT_EQUAL,
                new StringToken("banned"),
                SymbolToken.SEMICOLON,
                new EofToken()
        };

        assertEquals(expected.length, tokens.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], tokens.get(i), "Mismatch at index " + i);
        }
    }
}
