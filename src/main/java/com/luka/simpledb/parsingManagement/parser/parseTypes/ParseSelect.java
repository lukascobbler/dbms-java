package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/// The class responsible for parsing data queries.
/// Its subgrammar is defined like this:
///
/// ```
/// <TableName>                 := IdentificationToken
/// <ParseSelect>               := SELECT <SelectExpressionList> FROM <TableList> [WHERE<ParsePredicate>]
/// <SelectExpressionList>      := <ParseExpression> [, <SelectExpressionList>]
/// <TableList>                 := <TableName> [, <TableList>]
/// ```
public class ParseSelect {
    private final ParserContext ctx;

    public ParseSelect(ParserContext ctx) {
        this.ctx = ctx;
    }

    public SelectStatement parse() {
        ctx.eat(Keyword.SELECT);
        List<Expression> fields = selectExpressionList();

        ctx.eat(Keyword.FROM);
        Collection<String> tables = tableList();

        Predicate predicate = new Predicate();
        if (ctx.eatIfMatches(Keyword.WHERE)) {
            predicate = new ParsePredicate(ctx).parse();
        }

        ctx.eat(SymbolToken.SEMICOLON);

        return new SelectStatement(fields, tables, predicate);
    }

    private List<Expression> selectExpressionList() {
        List<Expression> selectList = new ArrayList<>();

        do {
            Expression selectionExpression = new ParseExpression(ctx).parse();
            selectList.add(selectionExpression);
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
