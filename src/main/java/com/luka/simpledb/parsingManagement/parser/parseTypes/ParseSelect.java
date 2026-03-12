package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

import java.util.*;

/// The class responsible for parsing data queries.
/// Its subgrammar is defined like this:
///
/// ```
/// <Field>                     := IdentificationToken
/// <TableName>                 := IdentificationToken
/// <ParseSelect>               := SELECT <SelectExpressionList> FROM <TableList> [WHERE<ParsePredicate>]
/// <SelectExpressionMap>       := <ParseExpression> [AS <Field>] [, <SelectExpressionList>]
/// <JoinSpecs>                 := <TableName> [<JoinSpec>] [, <JoinSpecs>]
/// <JoinSpec>                  := JOIN <TableName> ON <ParsePredicate>
/// ```
public class ParseSelect {
    private final ParserContext ctx;

    public ParseSelect(ParserContext ctx) {
        this.ctx = ctx;
    }

    public SelectStatement parse() {
        ctx.eat(Keyword.SELECT);
        Map<String, Expression> fields = selectExpressionMap();

        ctx.eat(Keyword.FROM);
        List<JoinSpec> joinSpecs = joinSpecs();

        Predicate predicate = new Predicate();
        if (ctx.eatIfMatches(Keyword.WHERE)) {
            predicate = new ParsePredicate(ctx).parse();
        }

        List<String> tables = new ArrayList<>();
        for (JoinSpec joinSpec : joinSpecs) {
            tables.addAll(joinSpec.tables);
            predicate.conjoinWith(joinSpec.joinPredicate);
        }

        return new SelectStatement(fields, tables, predicate);
    }

    private Map<String, Expression> selectExpressionMap() {
        Map<String, Expression> selectMap = new HashMap<>();

        // todo predicates instead of expressions here
        do {
            String newFieldName;

            Expression selectionExpression = new ParseExpression(ctx).parse();

            if (ctx.eatIfMatches(Keyword.AS)) {
                newFieldName = ctx.eatIdentifier();
            } else {
                newFieldName = selectionExpression.toString();
            }

            selectMap.put(newFieldName, selectionExpression);
        } while (ctx.eatIfMatches(SymbolToken.COMMA));

        return selectMap;
    }

    private List<JoinSpec> joinSpecs() {
        List<JoinSpec> joinSpecList = new ArrayList<>();

        do {
            joinSpecList.add(joinSpec());
        } while (ctx.eatIfMatches(SymbolToken.COMMA));

        return joinSpecList;
    }

    private JoinSpec joinSpec() {
        Predicate joinPredicate = new Predicate();

        List<String> tables = new ArrayList<>();
        tables.add(tableName());

        if (ctx.eatIfMatches(Keyword.JOIN)) {
            tables.add(tableName());
            ctx.eat(Keyword.ON);
            joinPredicate.conjoinWith(new ParsePredicate(ctx).parse());
        }

        return new JoinSpec(tables, joinPredicate);
    }

    private String tableName() {
        return ctx.eatIdentifier();
    }

    private String newFieldName() {
        return ctx.eatIdentifier();
    }

    private record JoinSpec(List<String> tables, Predicate joinPredicate) { }
}
