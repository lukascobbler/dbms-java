package com.luka.lbdb.parsing.parser;

import com.luka.lbdb.parsing.exceptions.ParsingException;
import com.luka.lbdb.parsing.tokenizer.Tokenizer;
import com.luka.lbdb.parsing.tokenizer.token.*;

import java.util.Optional;

/// A parser context is a wrapper around the tokenizer that
/// redefines some behavior like advancing and defines shared
/// operations for consuming tokens in different ways.
/// This context should be shared between all parse classes
/// to ensure that all of them have the most up-to-date token
/// as the last token.
public class ParserContext {
    private final Tokenizer tokenizer;
    private Token currentToken;

    /// A parser context is initialized from a query, so that it
    /// can initialize a tokenizer from that query.
    public ParserContext(String query) {
        this.tokenizer = new Tokenizer(query);
        advance();
    }

    /// @return The current token.
    public Token current() {
        return currentToken;
    }

    /// Advances the token stream by one. If there are no more tokens,
    /// sets the current one to an `EofToken`. This operation can be called
    /// forever without failing.
    ///
    /// @return The previous token.
    public Token advance() {
        Token oldToken = currentToken;
        if (tokenizer.hasNext()) {
            currentToken = tokenizer.next();
        } else {
            currentToken = new EofToken();
        }
        return oldToken;
    }

    /// Checks if the current token is an identifier, and if it is,
    /// returns it.
    ///
    /// @return The identifier.
    /// @throws ParsingException if the current token isn't an identifier.
    public String eatIdentifier() {
        if (currentToken instanceof IdentifierToken(String name)) {
            advance();
            return name;
        }
        throw new ParsingException("Expected identifier but found '" + currentToken + "'");
    }

    /// Checks if the current token is an identifier, and if it is,
    /// returns it. If it is not, returns an empty value.
    ///
    /// @return The optional identifier.
    public Optional<String> eatIdentifierIfMatches() {
        if (!(currentToken instanceof IdentifierToken)) {
            return Optional.empty();
        }
        return Optional.of(eatIdentifier());
    }

    /// Consumes the current token and advances to the next one
    /// only if it matches `expected`.
    ///
    /// @return Whether the current token matches the passed one.
    public boolean eatIfMatches(Token expected) {
        if (currentToken.equals(expected)) {
            advance();
            return true;
        }
        return false;
    }

    /// Consumes the current token and advances to the next one
    /// only if it matches `expected`. If it doesn't match, errors.
    ///
    /// @throws ParsingException if the passed token doesn't match the
    /// current token.
    public void eat(Token expected) {
        if (!eatIfMatches(expected)) {
            throw new ParsingException("Expected '" + expected + "' but found '" + currentToken + "'");
        }
    }
}
