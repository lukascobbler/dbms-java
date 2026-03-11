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

public class ParseInsert {
    private final ParserContext ctx;

    public ParseInsert(ParserContext ctx) {
        this.ctx = ctx;
    }

    public InsertStatement parse() {
        ctx.eatKeyword(Keyword.INTO);

        String tableName = ctx.eatIdentifier();

        ctx.eatDelimiter(SymbolToken.LEFT_PAREN);
        List<String> fields = fieldList();
        ctx.eatDelimiter(SymbolToken.RIGHT_PAREN);

        ctx.eatKeyword(Keyword.VALUES);

        ctx.eatDelimiter(SymbolToken.LEFT_PAREN);
        List<Constant> values = constantList();
        ctx.eatDelimiter(SymbolToken.RIGHT_PAREN);

        return new InsertStatement(tableName, fields, values);
    }

    private List<Constant> constantList() {
        List<Constant> constantList = new ArrayList<>();

        do {
            Expression constantExpression = new ParseExpression(ctx).parse();

            if (!constantExpression.isConstant()) {
                throw new ParserException("An insert statement must have constant expressions for new values");
            }

            Expression foldedExpr = PartialEvaluator.evaluate(constantExpression);

            constantList.add(foldedExpr.evaluate(null));
        } while (ctx.eatIfMatches(SymbolToken.COMMA));

        return constantList;
    }

    private List<String> fieldList() {
        List<String> fieldList = new ArrayList<>();

        do {
            fieldList.add(ctx.eatIdentifier());
        } while (ctx.eatIfMatches(SymbolToken.COMMA));

        return fieldList;
    }
}
