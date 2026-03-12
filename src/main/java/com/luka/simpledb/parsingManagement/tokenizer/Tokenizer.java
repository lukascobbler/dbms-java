package com.luka.simpledb.parsingManagement.tokenizer;

import com.luka.simpledb.parsingManagement.exceptions.TokenizationException;
import com.luka.simpledb.parsingManagement.tokenizer.token.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

/// A tokenizer is a parsing concept that converts a character stream (the string query)
/// into a more grouped concept called tokens. Tokens are syntactically more valuable
/// than characters because they group these characters together into meaningful wholes.
/// The `Tokenizer` class is implemented as an iterator, to allow streamlined operations
/// and efficiency.
public class Tokenizer implements Iterator<Token> {
    private final char[] input;
    private int pos = 0;
    private boolean isEofReached = false;

    public Tokenizer(String query) {
        this.input = query.toCharArray();
    }

    @Override
    public boolean hasNext() {
        return !isEofReached;
    }

    @Override
    public Token next() {
        if (isEofReached) {
            throw new NoSuchElementException();
        }

        skipWhitespace();

        if (isAtEnd()) {
            isEofReached = true;
            return new EofToken();
        }

        char c = advance();

        return switch (c) {
            case '=' -> SymbolToken.EQUAL;
            case '+' -> SymbolToken.PLUS;
            case '-' -> SymbolToken.MINUS;
            case '/' -> SymbolToken.DIVIDE;
            case ',' -> SymbolToken.COMMA;
            case '.' -> SymbolToken.DOT;
            case '(' -> SymbolToken.LEFT_PAREN;
            case ')' -> SymbolToken.RIGHT_PAREN;
            case ';' -> SymbolToken.SEMICOLON;
            case '!' -> match('=')
                    ? SymbolToken.NOT_EQUAL
                    : new InvalidToken("!");
            case '>' -> match('=')
                    ? SymbolToken.GREATER_THAN_OR_EQUAL
                    : SymbolToken.GREATER_THAN;
            case '<' -> match('=')
                    ? SymbolToken.LESS_THAN_OR_EQUAL
                    : SymbolToken.LESS_THAN;
            case '*' -> SymbolToken.STAR;
            case '"', '\'' -> buildString(c);
            default -> {
                if (Character.isDigit(c)) {
                    yield buildNumber(c);
                } else if (isKeywordOrIdentifierChar(c)) {
                    yield buildKeywordOrIdentifier(c);
                } else {
                    yield new InvalidToken(String.valueOf(c));
                }
            }
        };
    }

    private Token buildKeywordOrIdentifier(char startingChar) {
        int startPos = pos - 1;

        while (!isAtEnd() && isKeywordOrIdentifierChar(peek())) {
            advance();
        }

        String text = new String(input, startPos, pos - startPos);
        Keyword keyword = Keyword.fromString(text);

        if (keyword != null) {
            return new KeywordToken(keyword);
        }

        return new IdentifierToken(text.toLowerCase());
    }

    private Token buildString(char startingQuote) {
        int startPos = pos;

        while (!isAtEnd() && peek() != startingQuote) {
            advance();
        }

        if (isAtEnd()) {
            throw new TokenizationException();
        }

        String text = new String(input, startPos, pos - startPos);
        advance();
        return new StringToken(text);
    }

    private Token buildNumber(char startingChar) {
        int startPos = pos - 1;

        while (!isAtEnd() && Character.isDigit(peek())) {
            advance();
        }

        String numStr = new String(input, startPos, pos - startPos);

        return new IntegerToken(Integer.parseInt(numStr));
    }

    private boolean isKeywordOrIdentifierChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private boolean match(char expected) {
        if (isAtEnd() || input[pos] != expected) {
            return false;
        }
        pos++;
        return true;
    }

    private char peek() {
        if (isAtEnd()) return '\0';
        return input[pos];
    }

    private char advance() {
        return input[pos++];
    }

    private boolean isAtEnd() {
        return pos >= input.length;
    }

    private void skipWhitespace() {
        while (!isAtEnd() && Character.isWhitespace(peek())) {
            advance();
        }
    }
}