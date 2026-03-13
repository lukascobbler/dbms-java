package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.CreateViewStatement;
import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.token.KeywordToken;

/// The class responsible for parsing view creation.
/// Its subgrammar is defined like this:
///
/// ```
/// <ViewName>              := IdentificationToken
/// <ParseCreateView>       := VIEW <ViewName> AS <ParseSelect>
/// ```
public class ParseCreateView {
    private final ParserContext ctx;

    /// Every syntactic category requires the parse context to
    /// be initialized.
    public ParseCreateView(ParserContext ctx) {
        this.ctx = ctx;
    }

    /// Parses the view creation statement according to the sub-grammar
    /// defined in the class documentation.
    ///
    /// @return Parsed data required for view creation.
    public CreateViewStatement parse() {
        ctx.eat(KeywordToken.VIEW);
        String viewName = viewName();
        ctx.eat(KeywordToken.AS);
        SelectStatement selectStatement = new ParseSelect(ctx).parse();

        return new CreateViewStatement(viewName, selectStatement);
    }

    /// @return The view name identifier string.
    private String viewName() {
        return ctx.eatIdentifier();
    }
}
