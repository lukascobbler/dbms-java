package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.statement.UpdateStatement;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.KeywordToken;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;
import com.luka.simpledb.queryManagement.virtualEntities.expression.PartialEvaluator;

/// The class responsible for parsing row updates.
/// Its subgrammar is defined like this:
///
/// ```
/// <TableName>         := IdentificationToken
/// <FieldName>         := IdentificationToken
/// <ParseUpdate>       := UPDATE <TableName> SET <FieldName> = <ParseExpression> [WHERE<ParsePredicate>]
/// ```
public class ParseUpdate {
    private final ParserContext ctx;

    public ParseUpdate(ParserContext ctx) {
        this.ctx = ctx;
    }

    public UpdateStatement parse() {
        ctx.eat(Keyword.UPDATE);
        String tableName = tableName();
        ctx.eat(Keyword.SET);

        String fieldName = fieldName();
        ctx.eat(SymbolToken.EQUAL);

        Expression newValue = new ParseExpression(ctx).parse();

        // todo folding here or somewhere else
        Expression newValueFolded = PartialEvaluator.evaluate(newValue);

        Predicate predicate = new Predicate();
        if (ctx.eatIfMatches(Keyword.WHERE)) {
            predicate = new ParsePredicate(ctx).parse();
        }

        return new UpdateStatement(tableName, fieldName, newValueFolded, predicate);
    }

    public String tableName() {
        return ctx.eatIdentifier();
    }

    public String fieldName() {
        return ctx.eatIdentifier();
    }
}
