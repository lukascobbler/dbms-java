package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.KeywordToken;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/// The class responsible for parsing data queries.
/// Its subgrammar is defined like this:
///
/// ```
/// <TableName>         := IdentificationToken
/// <ParseSelect>       := SELECT <SelectList> FROM <TableList> [WHERE<ParsePredicate>]
/// <SelectList>        := <ParseExpression> [, <SelectList>]
/// <TableList>         := <TableName> [, <TableList>]
/// ```
public class ParseSelect {
    private final ParserContext ctx;

    public ParseSelect(ParserContext ctx) {
        this.ctx = ctx;
    }

    public SelectStatement parse() {
        ctx.eat(Keyword.SELECT);
        List<String> fields = selectList();

        ctx.eat(Keyword.FROM);
        Collection<String> tables = tableList();

        Predicate predicate = new Predicate();
        if (ctx.eatIfMatches(Keyword.WHERE)) {
            predicate = new ParsePredicate(ctx).parse();
        }

        ctx.eat(SymbolToken.SEMICOLON);

        return new SelectStatement(fields, tables, predicate);
    }

    private List<String> selectList() {
        List<String> selectList = new ArrayList<>();

        // todo selection expressions
        do {
            selectList.add(ctx.eatIdentifier());
        } while (ctx.eatIfMatches(SymbolToken.COMMA));

        return selectList;
    }

    private Collection<String> tableList() {
        Collection<String> tableList = new ArrayList<>();

        do {
            tableList.add(tableName());
        } while (ctx.eatIfMatches(SymbolToken.COMMA));

        return tableList;
    }

    private String tableName() {
        return ctx.eatIdentifier();
    }
}
