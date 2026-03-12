package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.statement.SelectStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.statement.select.ProjectionFieldInfo;
import com.luka.simpledb.parsingManagement.statement.select.SingleSelection;
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
/// <ParseSelect>               := <ParseSingleSelection> [UNION <ParseSingleSelection>]
/// <ParseSingleSelection>      := SELECT <ProjectedFields> FROM <JoinSpecs> [WHERE<ParsePredicate>]
/// <ProjectedFields>           := <ParseExpression> [AS <Field>] [, <ProjectedFields>]
/// <JoinSpecs>                 := <TableName> [<JoinSpec>] [, <JoinSpecs>]
/// <JoinSpec>                  := JOIN <TableName> ON <ParsePredicate>
/// ```
public class ParseSelect {
    private final ParserContext ctx;

    public ParseSelect(ParserContext ctx) {
        this.ctx = ctx;
    }

    public SelectStatement parse() {
        List<SingleSelection> singleSelections = new ArrayList<>();

        do {
            ctx.eat(Keyword.SELECT);
            List<ProjectionFieldInfo> fields = projectedFields();

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

            singleSelections.add(new SingleSelection(fields, tables, predicate));
        } while (ctx.eatIfMatches(Keyword.UNION));

        return new SelectStatement(singleSelections);
    }

    private List<ProjectionFieldInfo> projectedFields() {
        List<ProjectionFieldInfo> projectList = new ArrayList<>();

        // todo predicates instead of expressions here
        // todo what happens with "*" fields
        // todo what happens with duplicate field names (handle in JDBC? with field number accesses)
        do {
            String newFieldName;

            Expression projectionExpression = new ParseExpression(ctx).parse();

            if (ctx.eatIfMatches(Keyword.AS)) {
                newFieldName = ctx.eatIdentifier();
            } else {
                newFieldName = projectionExpression.toString();
            }

            projectList.add(new ProjectionFieldInfo(newFieldName, projectionExpression));
        } while (ctx.eatIfMatches(SymbolToken.COMMA));

        return projectList;
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
