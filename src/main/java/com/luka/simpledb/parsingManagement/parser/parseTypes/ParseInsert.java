package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.exceptions.ParserException;
import com.luka.simpledb.parsingManagement.statement.InsertStatement;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;
import com.luka.simpledb.queryManagement.virtualEntities.constant.Constant;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;
import com.luka.simpledb.queryManagement.virtualEntities.expression.PartialEvaluator;

import java.util.ArrayList;
import java.util.List;

/// The class responsible for parsing row insertion.
/// Its subgrammar is defined like this:
///
/// ```
/// <TableName>         := IdentificationToken
/// <FieldName>         := IdentificationToken
/// <ParseInsert>       := INTO <TableName> (<FieldList>) VALUES (<ConstantList>)
/// <FieldList>         := <FieldName> [, <FieldList>]
/// <ConstantList>      := <Constant> [, <ConstantList>]
/// ```
public class ParseInsert {
    private final ParserContext ctx;

    public ParseInsert(ParserContext ctx) {
        this.ctx = ctx;
    }

    public InsertStatement parse() {
        ctx.eat(Keyword.INSERT);
        ctx.eat(Keyword.INTO);

        String tableName = tableName();

        ctx.eat(SymbolToken.LEFT_PAREN);
        List<String> fields = fieldList();
        ctx.eat(SymbolToken.RIGHT_PAREN);

        ctx.eat(Keyword.VALUES);

        ctx.eat(SymbolToken.LEFT_PAREN);
        List<Constant> values = constantList();
        ctx.eat(SymbolToken.RIGHT_PAREN);

        return new InsertStatement(tableName, fields, values);
    }

    private List<Constant> constantList() {
        List<Constant> constantList = new ArrayList<>();

        do {
            Expression constantExpression = new ParseExpression(ctx).parse();

            if (!constantExpression.isConstant()) {
                throw new ParserException("An insert statement must have constant expressions for new values");
            }

            // todo folding here or after
            Expression foldedExpr = PartialEvaluator.evaluate(constantExpression);

            constantList.add(foldedExpr.evaluate(null));
        } while (ctx.eatIfMatches(SymbolToken.COMMA));

        return constantList;
    }

    private List<String> fieldList() {
        List<String> fieldList = new ArrayList<>();

        do {
            fieldList.add(fieldName());
        } while (ctx.eatIfMatches(SymbolToken.COMMA));

        return fieldList;
    }

    private String tableName() {
        return ctx.eatIdentifier();
    }

    private String fieldName() {
        return ctx.eatIdentifier();
    }
}
