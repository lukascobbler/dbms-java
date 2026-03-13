package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.DeleteStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.token.KeywordToken;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;

/// The class responsible for parsing row deletion.
/// Its subgrammar is defined like this:
///
/// ```
/// <TableName>         := IdentificationToken
/// <ParseDelete>       := DELETE FROM <TableName> [WHERE<ParsePredicate>]
/// ```
public class ParseDelete {
    private final ParserContext ctx;

    /// Every syntactic category requires the parse context to
    /// be initialized.
    public ParseDelete(ParserContext ctx) {
        this.ctx = ctx;
    }

    /// Parses the delete  statement according to the sub-grammar
    /// defined in the class documentation.
    ///
    /// @return Parsed data required for data deletion.
    public DeleteStatement parse() {
        ctx.eat(KeywordToken.DELETE);
        ctx.eat(KeywordToken.FROM);

        String tableName = tableName();
        Predicate predicate = new Predicate();

        if (ctx.eatIfMatches(KeywordToken.WHERE)) {
            predicate = new ParsePredicate(ctx).parse();
        }

        return new DeleteStatement(tableName, predicate);
    }

    /// @return The table name identifier string.
    private String tableName() {
        return ctx.eatIdentifier();
    }
}
