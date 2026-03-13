package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.statement.UpdateStatement;
import com.luka.simpledb.parsingManagement.statement.update.NewFieldExpressionAssignment;
import com.luka.simpledb.parsingManagement.tokenizer.token.KeywordToken;
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

    /// Every syntactic category requires the parse context to
    /// be initialized.
    public ParseUpdate(ParserContext ctx) {
        this.ctx = ctx;
    }

    /// Parses the update statement according to the sub-grammar
    /// defined in the class documentation.
    ///
    /// @return Parsed data required for data updating.
    public UpdateStatement parse() {
        ctx.eat(KeywordToken.UPDATE);
        String tableName = tableName();
        ctx.eat(KeywordToken.SET);

        List<NewFieldExpressionAssignment> newFieldValues = setNewValues();

        Predicate predicate = new Predicate();
        if (ctx.eatIfMatches(KeywordToken.WHERE)) {
            predicate = new ParsePredicate(ctx).parse();
        }

        return new UpdateStatement(tableName, newFieldValues, predicate);
    }

    /// @return The list of field names and their new value expressions
    /// separated by commas as identifiers.
    private List<NewFieldExpressionAssignment> setNewValues() {
        List<NewFieldExpressionAssignment> newFieldValues = new ArrayList<>();

        do {
            newFieldValues.add(setNewValue());
        } while (ctx.eatIfMatches(SymbolToken.COMMA));

        return newFieldValues;
    }

    /// @return A field that equals some expression.
    private NewFieldExpressionAssignment setNewValue() {
        String fieldName = fieldName();

        ctx.eat(SymbolToken.EQUAL);

        Expression newValueExpression = new ParseExpression(ctx).parse();

        return new NewFieldExpressionAssignment(fieldName, newValueExpression);
    }

    /// @return The table name identifier string.
    private String tableName() {
        return ctx.eatIdentifier();
    }

    /// @return The field name identifier string.
    private String fieldName() {
        return ctx.eatIdentifier();
    }
}
