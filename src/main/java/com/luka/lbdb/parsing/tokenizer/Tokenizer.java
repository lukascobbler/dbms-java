package com.luka.lbdb.parsing.tokenizer;

import com.luka.lbdb.parsing.exceptions.TokenizationException;
import com.luka.lbdb.parsing.tokenizer.token.*;

import java.util.Arrays;
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

    /// A tokenizer is initialized from a query.
    public Tokenizer(String query) {
        this.input = query.toCharArray();
    }

    /// @return True if the end of the query isn't reached.
    @Override
    public boolean hasNext() {
        return !isEofReached;
    }

    /// Processes the character input up until the next whole token.
    /// Advances the position for the next token to be processable.
    ///
    /// @return The next whole token from the query.
    /// @throws NoSuchElementException if there are no more tokens in the
    /// stream.
    /// @throws TokenizationException if any set of characters that is supposed
    /// to be a token couldn't be converted into one.
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
            case '(' -> SymbolToken.LEFT_PAREN;
            case ')' -> SymbolToken.RIGHT_PAREN;
            case ';' -> SymbolToken.SEMICOLON;
            case '*' -> SymbolToken.STAR;
            case '^' -> SymbolToken.CARET;
            case '.' -> SymbolToken.DOT;
            case '!' -> eatIfMatches('=') ? SymbolToken.NOT_EQUAL : new InvalidToken("!");
            case '>' -> eatIfMatches('=') ? SymbolToken.GREATER_THAN_OR_EQUAL : SymbolToken.GREATER_THAN;
            case '<' -> eatIfMatches('=') ? SymbolToken.LESS_THAN_OR_EQUAL : SymbolToken.LESS_THAN;
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

    /// Advances the internal character buffer until keyword and identifier
    /// chars are present and builds a keyword if those chars match one, else
    /// builds an identifier.
    ///
    /// @return The built keyword or identifier token.
    private Token buildKeywordOrIdentifier(char startingChar) {
        int startPos = pos - 1;

        while (!isAtEnd() && isKeywordOrIdentifierChar(peek())) {
            advance();
        }

        String text = new String(input, startPos, pos - startPos);

        return Arrays.stream(KeywordToken.values())
                .filter(t -> t.name().equalsIgnoreCase(text))
                .findFirst()
                .map(Token.class::cast)
                .orElse(new IdentifierToken(text.toLowerCase()));
    }

    /// Advances the internal character buffer until an end quote character,
    /// that must be the same as the beginning quote character, is reached.
    ///
    /// @return The string token that is between two quotes.
    /// @throws TokenizationException if a string isn't properly closed with
    /// a quote character.
    private Token buildString(char startingQuote) {
        int startPos = pos;

        while (!isAtEnd() && peek() != startingQuote) {
            advance();
        }

        if (isAtEnd()) {
            throw new TokenizationException("Found no closing quote (" + startingQuote + ") for a string");
        }

        String text = new String(input, startPos, pos - startPos);
        advance();
        return new StringToken(text);
    }

    /// Advances the internal character buffer until digits are present
    /// and builds a number from those digits.
    ///
    /// @return The built number token.
    private Token buildNumber(char startingChar) {
        int startPos = pos - 1;

        while (!isAtEnd() && Character.isDigit(peek())) {
            advance();
        }

        String numStr = new String(input, startPos, pos - startPos);

        return new IntegerToken(Integer.parseInt(numStr));
    }

    /// Only letters, digits and underscores are valid keyword and
    /// identifier characters.
    ///
    /// @return Whether a character is valid for keywords or identifiers.
    private boolean isKeywordOrIdentifierChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    /// Consumes the current character and advances to the next one
    /// only if it matches `expected`.
    ///
    /// @return Whether the current character matches the passed one.
    private boolean eatIfMatches(char expected) {
        if (isAtEnd() || input[pos] != expected) {
            return false;
        }
        pos++;
        return true;
    }

    /// Look at the current character without moving to the next one.
    ///
    /// @return The current char.
    private char peek() {
        if (isAtEnd()) return '\0';
        return input[pos];
    }

    /// Look at the current character and advance to the next one.
    ///
    /// @return The current char.
    private char advance() {
        return input[pos++];
    }

    /// @return Whether the current character position reached the
    /// end of the query input.
    private boolean isAtEnd() {
        return pos >= input.length;
    }

    /// Advance the internal character buffer until a non-whitespace
    /// character is reached.
    private void skipWhitespace() {
        while (!isAtEnd() && Character.isWhitespace(peek())) {
            advance();
        }
    }
}