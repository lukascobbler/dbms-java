package com.luka.simpledb.parsingManagement.parser;

import com.luka.simpledb.parsingManagement.exceptions.ParserException;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.Tokenizer;
import com.luka.simpledb.parsingManagement.tokenizer.token.*;

public class ParserContext {
    private final Tokenizer tokenizer;
    private Token currentToken;

    public ParserContext(String query) {
        this.tokenizer = new Tokenizer(query);
        advance();
    }

    public Token current() {
        return currentToken;
    }

    public void advance() {
        if (tokenizer.hasNext()) {
            currentToken = tokenizer.next();
        } else {
            currentToken = new EofToken();
        }
    }

    public void eat(Keyword expected) {
        eat(new KeywordToken(expected));
    }

    public void eat(SymbolToken expected) {
        eat((Token) expected);
    }

    public boolean eatIfMatches(Keyword expected) {
        return eatIfMatches(new KeywordToken(expected));
    }

    public boolean eatIfMatches(SymbolToken expected) {
        return eatIfMatches((Token) expected);
    }

    /// Checks if the current token is an identifier, and if it is,
    /// returns it.
    ///
    /// @return The identifier.
    /// @throws ParserException if the current token isn't an identifier.
    public String eatIdentifier() {
        if (currentToken instanceof IdentifierToken(String name)) {
            advance();
            return name;
        }
        throw new ParserException("Syntax Error: Expected identifier but found '" + currentToken + "'");
    }

    private boolean eatIfMatches(Token expected) {
        if (currentToken.equals(expected)) {
            advance();
            return true;
        }
        return false;
    }

    private void eat(Token expected) {
        if (!eatIfMatches(expected)) {
            throw new ParserException("Syntax Error: Expected '" + expected + "' but found '" + currentToken + "'");
        }
    }
}
