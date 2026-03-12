package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.DeleteStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
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

    public ParseDelete(ParserContext ctx) {
        this.ctx = ctx;
    }

    public DeleteStatement parse() {
        ctx.eat(Keyword.DELETE);
        ctx.eat(Keyword.FROM);

        String tableName = tableName();
        Predicate predicate = new Predicate();

        if (ctx.eatIfMatches(Keyword.WHERE)) {
            predicate = new ParsePredicate(ctx).parse();
        }

        return new DeleteStatement(tableName, predicate);
    }

    private String tableName() {
        return ctx.eatIdentifier();
    }
}
