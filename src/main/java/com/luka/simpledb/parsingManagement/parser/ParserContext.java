package com.luka.simpledb.parsingManagement.parser;

import com.luka.simpledb.parsingManagement.exceptions.BadSyntaxException;
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

    public boolean eatIfMatches(Token expected) {
        if (currentToken.equals(expected)) {
            advance();
            return true;
        }
        return false;
    }

    public void eatKeyword(Keyword expected) {
        if (currentToken instanceof KeywordToken(Keyword kw) && kw == expected) {
            advance();
        } else {
            throw new BadSyntaxException();
        }
    }

    public void eatDelimiter(SymbolToken expected) {
        if (currentToken == expected) {
            advance();
        } else {
            throw new BadSyntaxException();
        }
    }

    public String eatIdentifier() {
        if (currentToken instanceof IdentifierToken(String name)) {
            advance();
            return name;
        }
        throw new BadSyntaxException();
    }

    public int eatIntConstant() {
        if (currentToken instanceof IntegerToken(int value)) {
            advance();
            return value;
        }
        throw new BadSyntaxException();
    }
}
