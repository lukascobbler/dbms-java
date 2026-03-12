package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.CreateViewStatement;
import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;

/// The class responsible for parsing view creation.
/// Its subgrammar is defined like this:
///
/// ```
/// <ViewName>              := IdentificationToken
/// <ParseCreateView>       := VIEW <ViewName> AS <ParseSelect>
/// ```
public class ParseCreateView {
    private final ParserContext ctx;

    public ParseCreateView(ParserContext ctx) {
        this.ctx = ctx;
    }

    public CreateViewStatement parse() {
        ctx.eat(Keyword.VIEW);
        String viewName = viewName();
        ctx.eat(Keyword.AS);
        SelectStatement selectStatement = new ParseSelect(ctx).parse();

        return new CreateViewStatement(viewName, selectStatement);
    }

    private String viewName() {
        return ctx.eatIdentifier();
    }
}
