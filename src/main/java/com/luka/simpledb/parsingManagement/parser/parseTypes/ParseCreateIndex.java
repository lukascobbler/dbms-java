package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.CreateIndexStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;

public class ParseCreateIndex {
    private final ParserContext ctx;

    public ParseCreateIndex(ParserContext ctx) {
        this.ctx = ctx;
    }

    public CreateIndexStatement parse() {
        String indexName = ctx.eatIdentifier();
        ctx.eatKeyword(Keyword.ON);
        String tableName = ctx.eatIdentifier();
        ctx.eatDelimiter(SymbolToken.LEFT_PAREN);
        String fieldName = ctx.eatIdentifier();
        ctx.eatDelimiter(SymbolToken.RIGHT_PAREN);

        return new CreateIndexStatement(indexName, tableName, fieldName);
    }
}
