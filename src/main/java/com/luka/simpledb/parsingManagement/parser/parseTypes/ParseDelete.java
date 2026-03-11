package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.DeleteStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.KeywordToken;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;

public class ParseDelete {
    private final ParserContext ctx;

    public ParseDelete(ParserContext ctx) {
        this.ctx = ctx;
    }

    public DeleteStatement parse() {
        ctx.eatKeyword(Keyword.FROM);

        String tableName = ctx.eatIdentifier();
        Predicate predicate = new Predicate();

        if (ctx.eatIfMatches(new KeywordToken(Keyword.WHERE))) {
            predicate = new ParsePredicate(ctx).parse();
        }

        return new DeleteStatement(tableName, predicate);
    }
}
