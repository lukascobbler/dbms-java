package com.luka.simpledb.parsingManagement.parser.parseTypes;

import com.luka.simpledb.parsingManagement.exceptions.ParserException;
import com.luka.simpledb.parsingManagement.statement.CreateTableStatement;
import com.luka.simpledb.parsingManagement.exceptions.BadSyntaxException;
import com.luka.simpledb.parsingManagement.parser.ParserContext;
import com.luka.simpledb.parsingManagement.tokenizer.Keyword;
import com.luka.simpledb.parsingManagement.tokenizer.token.SymbolToken;
import com.luka.simpledb.queryManagement.exceptions.IncompatibleConstantTypeException;
import com.luka.simpledb.queryManagement.virtualEntities.expression.Expression;
import com.luka.simpledb.queryManagement.virtualEntities.expression.PartialEvaluator;
import com.luka.simpledb.recordManagement.Schema;

/// The class responsible for parsing table creation.
/// Its subgrammar is defined like this:
///
/// ```
/// <TableName>             := IdentificationToken
/// <FieldName>             := IdentificationToken
/// <ParseCreateTable>      := TABLE <TableName> (<FieldDefs>)
/// <FieldDefinitions>      := <FieldDefinition> [, <FieldDefinitions>]
/// <FieldDefinition>       := <FieldName> INT | VARCHAR (IntToken) | BOOLEAN [NOT NULL]
/// ```
public class ParseCreateTable {
    private final ParserContext ctx;

    public ParseCreateTable(ParserContext ctx) {
        this.ctx = ctx;
    }

    public CreateTableStatement parse() {
        ctx.eat(Keyword.TABLE);
        String tableName = tableName();

        ctx.eat(SymbolToken.LEFT_PAREN);
        Schema schema = fieldDefinitions();
        ctx.eat(SymbolToken.RIGHT_PAREN);

        return new CreateTableStatement(tableName, schema);
    }

    private Schema fieldDefinitions() {
        Schema schema = fieldDefinition();

        while (ctx.eatIfMatches(SymbolToken.COMMA)) {
            schema.addAll(fieldDefinition());
        }

        return schema;
    }

    private Schema fieldDefinition() {
        String fieldName = fieldName();
        Schema schema = new Schema();

        if (ctx.eatIfMatches(Keyword.INT)) {
            schema.addIntField(fieldName, isNullable());
        } else if (ctx.eatIfMatches(Keyword.VARCHAR)) {
            ctx.eat(SymbolToken.LEFT_PAREN);

            Expression constantExpression = new ParseExpression(ctx).parse();

            if (!constantExpression.isConstant()) {
                throw new ParserException("The varchar string length must be a constant expression");
            }

            // todo folding here or after
            Expression foldedExpr = PartialEvaluator.evaluate(constantExpression);

            int stringLength;
            try {
                stringLength = foldedExpr.evaluate(null).asInt();
            } catch (IncompatibleConstantTypeException e) {
                throw new ParserException("The varchar string length must be an integer");
            }

            ctx.eat(SymbolToken.RIGHT_PAREN);
            schema.addStringField(fieldName, stringLength, isNullable());
        } else if (ctx.eatIfMatches(Keyword.BOOLEAN)) {
            schema.addBooleanField(fieldName, isNullable());
        } else {
            throw new BadSyntaxException();
        }

        return schema;
    }

    private boolean isNullable() {
        if (ctx.eatIfMatches(Keyword.NOT)) {
            if (ctx.eatIfMatches(Keyword.NULL)) {
                return false;
            }
            throw new ParserException("\"NOT\" not followed by \"NULL\" in a create table constraint");
        }
        return true;
    }

    private String tableName() {
        return ctx.eatIdentifier();
    }

    private String fieldName() {
        return ctx.eatIdentifier();
    }
}
