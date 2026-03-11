package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.CreateViewStatement;
import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;

public class ParseCreateView {
    private final ParserContext ctx;

    public ParseCreateView(ParserContext ctx) {
        this.ctx = ctx;
    }

    public CreateViewStatement parse() {
        String viewName = ctx.eatIdentifier();
        ctx.eatKeyword(Keyword.AS);
        SelectStatement selectStatement = new ParseSelect(ctx).parse();

        return new CreateViewStatement(viewName, selectStatement);
    }
}
