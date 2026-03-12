package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.statement.UpdateStatement;
import com.luka.simpledb.parsingManagement.statement.update.NewFieldExpressionAssignment;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;
import com.luka.simpledb.queryManagement.virtualEntities.Predicate;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;

import java.util.ArrayList;
import java.util.List;

/// The class responsible for parsing row updates.
/// Its subgrammar is defined like this:
///
/// ```
/// <TableName>         := IdentificationToken
/// <FieldName>         := IdentificationToken
/// <ParseUpdate>       := UPDATE <TableName> SET <SetNewValues> [WHERE<ParsePredicate>]
/// <SetNewValues>      := <SetNewValue> [, <SetNewValue>]
/// <SetNewValue>       := <FieldName> = <ParseExpression>
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

        List<NewFieldExpressionAssignment> newFieldValues = setNewValues();

        Predicate predicate = new Predicate();
        if (ctx.eatIfMatches(Keyword.WHERE)) {
            predicate = new ParsePredicate(ctx).parse();
        }

        return new UpdateStatement(tableName, newFieldValues, predicate);
    }

    private List<NewFieldExpressionAssignment> setNewValues() {
        List<NewFieldExpressionAssignment> newFieldValues = new ArrayList<>();

        do {
            newFieldValues.add(setNewValue());
        } while (ctx.eatIfMatches(SymbolToken.COMMA));

        return newFieldValues;
    }

    private NewFieldExpressionAssignment setNewValue() {
        String fieldName = fieldName();

        ctx.eat(SymbolToken.EQUAL);

        Expression newValueExpression = new ParseExpression(ctx).parse();

        return new NewFieldExpressionAssignment(fieldName, newValueExpression);
    }

    private String tableName() {
        return ctx.eatIdentifier();
    }

    private String fieldName() {
        return ctx.eatIdentifier();
    }
}
