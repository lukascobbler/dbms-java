package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.statement.UpdateStatement;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.KeywordToken;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

public class ParseUpdate {
    private final ParserContext ctx;

    public ParseUpdate(ParserContext ctx) {
        this.ctx = ctx;
    }

    public UpdateStatement parse() {
        String tableName = ctx.eatIdentifier();
        ctx.eatKeyword(Keyword.SET);

        String fieldName = ctx.eatIdentifier();
        ctx.eatDelimiter(SymbolToken.EQUAL);

        Expression newValue = new ParseExpression(ctx).parse();

        Predicate predicate = new Predicate();
        if (ctx.eatIfMatches(new KeywordToken(Keyword.WHERE))) {
            predicate = new ParsePredicate(ctx).parse();
        }

        return new UpdateStatement(tableName, fieldName, newValue, predicate);
    }
}
