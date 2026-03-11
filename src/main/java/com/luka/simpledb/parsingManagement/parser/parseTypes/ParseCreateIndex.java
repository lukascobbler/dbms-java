package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.CreateIndexStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;

/// The class responsible for parsing index creation.
/// Its subgrammar is defined like this:
///
/// ```
/// <IndexName>             := IdentificationToken
/// <TableName>             := IdentificationToken
/// <FieldName>             := IdentificationToken
/// <ParseCreateIndex>      := INDEX <IndexName> ON <TableName> (<FieldName>)
/// ```
public class ParseCreateIndex {
    private final ParserContext ctx;

    public ParseCreateIndex(ParserContext ctx) {
        this.ctx = ctx;
    }

    public CreateIndexStatement parse() {
        ctx.eat(Keyword.INDEX);
        String indexName = indexName();
        ctx.eat(Keyword.ON);
        String tableName = tableName();
        ctx.eat(SymbolToken.LEFT_PAREN);
        String fieldName = fieldName();
        ctx.eat(SymbolToken.RIGHT_PAREN);

        return new CreateIndexStatement(indexName, tableName, fieldName);
    }

    private String indexName() {
        return ctx.eatIdentifier();
    }

    private String fieldName() {
        return ctx.eatIdentifier();
    }

    private String tableName() {
        return ctx.eatIdentifier();
    }
}
